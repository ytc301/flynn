package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	c "github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-check"
	"github.com/flynn/flynn/appliance/postgresql/state"
	ct "github.com/flynn/flynn/controller/types"
	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/pkg/postgres"
)

type PostgresSuite struct {
	Helper
}

var _ = c.ConcurrentSuite(&PostgresSuite{})

// Check postgres config to avoid regressing on https://github.com/flynn/flynn/issues/101
func (s *PostgresSuite) TestSSLRenegotiationLimit(t *c.C) {
	query := flynn(t, "/", "-a", "controller", "pg", "psql", "--", "-c", "SHOW ssl_renegotiation_limit")
	t.Assert(query, Succeeds)
	t.Assert(query, OutputContains, "ssl_renegotiation_limit \n-------------------------\n 0\n(1 row)")
}

func (s *PostgresSuite) TestDumpRestore(t *c.C) {
	r := s.newGitRepo(t, "empty")
	t.Assert(r.flynn("create"), Succeeds)

	t.Assert(r.flynn("resource", "add", "postgres"), Succeeds)

	t.Assert(r.flynn("pg", "psql", "--", "-c",
		"CREATE table foos (data text); INSERT INTO foos (data) VALUES ('foobar')"), Succeeds)

	file := filepath.Join(t.MkDir(), "db.dump")
	t.Assert(r.flynn("pg", "dump", "-f", file), Succeeds)
	t.Assert(r.flynn("pg", "psql", "--", "-c", "DROP TABLE foos"), Succeeds)

	r.flynn("pg", "restore", "-f", file)

	query := r.flynn("pg", "psql", "--", "-c", "SELECT * FROM foos")
	t.Assert(query, Succeeds)
	t.Assert(query, OutputContains, "foobar")
}

func (s *PostgresSuite) TestDeployNormalMode(t *c.C) {
	// create postgres app
	client := s.controllerClient(t)
	name := "postgres-deploy"
	app := &ct.App{Name: name, Strategy: "postgres"}
	t.Assert(client.CreateApp(app), c.IsNil)

	// copy release from default postgres app
	release, err := client.GetAppRelease("postgres")
	t.Assert(err, c.IsNil)
	release.ID = ""
	proc := release.Processes["postgres"]
	delete(proc.Env, "SINGLETON")
	proc.Env["FLYNN_POSTGRES"] = name
	proc.Service = name
	release.Processes["postgres"] = proc
	t.Assert(client.CreateRelease(release), c.IsNil)
	t.Assert(client.SetAppRelease(app.ID, release.ID), c.IsNil)
	oldRelease := release.ID

	// start 5 postgres and 2 web processes
	discEvents := make(chan *discoverd.Event)
	discStream, err := s.discoverdClient(t).Service(name).Watch(discEvents)
	t.Assert(err, c.IsNil)
	defer discStream.Close()
	jobEvents := make(chan *ct.JobEvent)
	jobStream, err := client.StreamJobEvents(name, 0, jobEvents)
	t.Assert(err, c.IsNil)
	defer jobStream.Close()
	t.Assert(client.PutFormation(&ct.Formation{
		AppID:     app.ID,
		ReleaseID: release.ID,
		Processes: map[string]int{"postgres": 5, "web": 2},
	}), c.IsNil)

	// watch cluster state changes
	type stateChange struct {
		state *state.State
		err   error
	}
	stateCh := make(chan stateChange)
	go func() {
		for event := range discEvents {
			if event.Kind != discoverd.EventKindServiceMeta {
				continue
			}
			var state state.State
			if err := json.Unmarshal(event.ServiceMeta.Data, &state); err != nil {
				stateCh <- stateChange{err: err}
				return
			}
			primary := ""
			if state.Primary != nil {
				primary = state.Primary.Addr
			}
			sync := ""
			if state.Sync != nil {
				sync = state.Sync.Addr
			}
			var async []string
			for _, a := range state.Async {
				async = append(async, a.Addr)
			}
			debugf(t, "got pg cluster state: primary=%s sync=%s async=%s", primary, sync, strings.Join(async, ","))
			stateCh <- stateChange{state: &state}
		}
	}()

	// wait for correct cluster state and number of web processes
	var pgState state.State
	var webJobs int
	ready := func() bool {
		return webJobs == 2 &&
			pgState.Primary != nil &&
			pgState.Sync != nil &&
			len(pgState.Async) == 3
	}
	for {
		if ready() {
			break
		}
		select {
		case s := <-stateCh:
			t.Assert(s.err, c.IsNil)
			pgState = *s.state
		case e, ok := <-jobEvents:
			if !ok {
				t.Fatalf("job event stream closed: %s", jobStream.Err())
			}
			debugf(t, "got job event: %s %s %s", e.Type, e.JobID, e.State)
			if e.Type == "web" && e.State == "up" {
				webJobs++
			}
		case <-time.After(30 * time.Second):
			t.Fatal("timed out waiting for postgres formation")
		}
	}

	// connect to the db so we can test writes
	db := postgres.Wait(name, fmt.Sprintf("dbname=postgres user=flynn password=%s", release.Env["PGPASSWORD"]))
	dbname := "deploy-test"
	t.Assert(db.Exec(fmt.Sprintf(`CREATE DATABASE "%s" WITH OWNER = "flynn"`, dbname)), c.IsNil)
	db, err = postgres.Open(name, fmt.Sprintf("dbname=%s user=flynn password=%s", dbname, release.Env["PGPASSWORD"]))
	t.Assert(err, c.IsNil)
	t.Assert(db.Exec(`CREATE TABLE deploy_test ( data text)`), c.IsNil)
	assertWriteable := func() {
		debug(t, "writing to postgres database")
		t.Assert(db.Exec(`INSERT INTO deploy_test (data) VALUES ('data')`), c.IsNil)
	}

	// check currently writeable
	assertWriteable()

	// check a deploy completes with expected cluster state changes
	release.ID = ""
	t.Assert(client.CreateRelease(release), c.IsNil)
	newRelease := release.ID
	deployment, err := client.CreateDeployment(app.ID, newRelease)
	t.Assert(err, c.IsNil)
	deployEvents := make(chan *ct.DeploymentEvent)
	deployStream, err := client.StreamDeployment(deployment.ID, deployEvents)
	t.Assert(err, c.IsNil)
	defer deployStream.Close()

	type expectedState struct {
		Primary, Sync string
		Async         []string
	}
	expected := []expectedState{
		// kill Async[0], new Async[2]
		{Primary: oldRelease, Sync: oldRelease, Async: []string{oldRelease, oldRelease}},
		{Primary: oldRelease, Sync: oldRelease, Async: []string{oldRelease, oldRelease, newRelease}},

		// kill Async[0], new Async[2]
		{Primary: oldRelease, Sync: oldRelease, Async: []string{oldRelease, newRelease}},
		{Primary: oldRelease, Sync: oldRelease, Async: []string{oldRelease, newRelease, newRelease}},

		// kill Async[0], new Async[2]
		{Primary: oldRelease, Sync: oldRelease, Async: []string{newRelease, newRelease}},
		{Primary: oldRelease, Sync: oldRelease, Async: []string{newRelease, newRelease, newRelease}},

		// kill Sync, new Async[2]
		{Primary: oldRelease, Sync: newRelease, Async: []string{newRelease, newRelease}},
		{Primary: oldRelease, Sync: newRelease, Async: []string{newRelease, newRelease, newRelease}},

		// kill Primary, new Async[2]
		{Primary: newRelease, Sync: newRelease, Async: []string{newRelease, newRelease}},
		{Primary: newRelease, Sync: newRelease, Async: []string{newRelease, newRelease, newRelease}},
	}

	assertNextState := func(expected expectedState) {
		var state state.State
		select {
		case s := <-stateCh:
			t.Assert(s.err, c.IsNil)
			state = *s.state
		case <-time.After(30 * time.Second):
			t.Fatal("timed out waiting for postgres cluster state")
		}
		if state.Primary == nil {
			t.Fatal("no primary configured")
		}
		if state.Primary.Meta["FLYNN_RELEASE_ID"] != expected.Primary {
			t.Fatal("primary has incorrect release")
		}
		if state.Sync == nil {
			t.Fatal("no sync configured")
		}
		if state.Sync.Meta["FLYNN_RELEASE_ID"] != expected.Sync {
			t.Fatal("sync has incorrect release")
		}
		if len(state.Async) != len(expected.Async) {
			t.Fatalf("expected %d asyncs, got %d", len(expected.Async), len(state.Async))
		}
		for i, release := range expected.Async {
			if state.Async[i].Meta["FLYNN_RELEASE_ID"] != release {
				t.Fatalf("async[%d] has incorrect release", i)
			}
		}
	}
	var expectedIndex, newWebJobs int
loop:
	for {
		select {
		case e := <-deployEvents:
			switch e.Status {
			case "complete":
				break loop
			case "failed":
				t.Fatalf("deployment failed: %s", e.Error)
			}
			debugf(t, "got deployment event: %s %s", e.JobType, e.JobState)
			if e.JobState != "up" && e.JobState != "down" {
				continue
			}
			switch e.JobType {
			case "postgres":
				assertNextState(expected[expectedIndex])
				expectedIndex++
			case "web":
				if e.JobState == "up" && e.ReleaseID == newRelease {
					newWebJobs++
				}
			}
		case <-time.After(60 * time.Second):
			t.Fatal("timed out waiting for deployment")
		}
	}

	// check we have the correct number of new web jobs
	t.Assert(newWebJobs, c.Equals, 2)

	// check writeable now deploy is complete
	assertWriteable()
}
