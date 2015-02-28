package strategy

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/flynn/flynn/appliance/postgresql/client"
	"github.com/flynn/flynn/appliance/postgresql/state"
	ct "github.com/flynn/flynn/controller/types"
	"github.com/flynn/flynn/discoverd/client"
)

func postgres(d *Deploy) error {
	log := d.logger.New("fn", "postgres")
	log.Info("starting postgres deployment")

	loggedErr := func(e string) error {
		log.Error(e)
		return errors.New(e)
	}

	if d.serviceMeta == nil {
		return loggedErr("missing pg cluster state")
	}

	var state state.State
	log.Info("decoding pg cluster state")
	if err := json.Unmarshal(d.serviceMeta.Data, &state); err != nil {
		log.Error("error decoding pg cluster state", "err", err)
		return err
	}

	// abort if we are not deploying from a clean state
	if state.Primary == nil {
		return loggedErr("pg cluster state has no primary")
	}
	numProcs := 1 + len(state.Async)
	if state.Sync != nil {
		numProcs++
	}
	if numProcs != d.Processes["postgres"] {
		return loggedErr("pg cluster state does not match expected processes")
	}
	if d.newReleaseState["postgres"] > 0 {
		return loggedErr("pg cluster in unexpected state")
	}

	replaceInstance := func(inst *discoverd.Instance) error {
		log := log.New("job_id", inst.Meta["FLYNN_JOB_ID"])

		d.deployEvents <- ct.DeploymentEvent{
			ReleaseID: d.OldReleaseID,
			JobState:  "stopping",
			JobType:   "postgres",
		}
		pg := pgmanager.NewClient(inst.Addr)
		log.Info("stopping postgres")
		if err := pg.Stop(); err != nil {
			log.Error("error stopping postgres", "err", err)
			return err
		}
		log.Info("waiting for postgres to stop")
	loop:
		for {
			select {
			case event := <-d.serviceEvents:
				if event.Kind == discoverd.EventKindDown && event.Instance.ID == inst.ID {
					d.deployEvents <- ct.DeploymentEvent{
						ReleaseID: d.OldReleaseID,
						JobState:  "down",
						JobType:   "postgres",
					}
					break loop
				}
			case <-time.After(30 * time.Second):
				return loggedErr("timed out waiting for postgres to stop")
			}
		}
		log.Info("starting new instance")
		d.deployEvents <- ct.DeploymentEvent{
			ReleaseID: d.NewReleaseID,
			JobState:  "starting",
			JobType:   "postgres",
		}
		d.newReleaseState["postgres"]++
		if err := d.client.PutFormation(&ct.Formation{
			AppID:     d.AppID,
			ReleaseID: d.NewReleaseID,
			Processes: d.newReleaseState,
		}); err != nil {
			log.Error("error scaling postgres formation up by one", "err", err)
			return err
		}
		log.Info("waiting for new instance to come up")
		for {
			select {
			case event := <-d.serviceEvents:
				if event.Kind.Any(discoverd.EventKindUp, discoverd.EventKindUpdate) &&
					event.Instance.Meta != nil &&
					event.Instance.Meta["FLYNN_RELEASE_ID"] == d.NewReleaseID &&
					event.Instance.Meta["FLYNN_PROCESS_TYPE"] == "postgres" &&
					event.Instance.Meta["PG_ONLINE"] == "true" {
					d.deployEvents <- ct.DeploymentEvent{
						ReleaseID: d.NewReleaseID,
						JobState:  "up",
						JobType:   "postgres",
					}
					return nil
				}
			case <-time.After(30 * time.Second):
				return loggedErr("timed out waiting for new instance to come up")
			}
		}
	}

	for i := 0; i < len(state.Async); i++ {
		log.Info("replacing an Async node")
		if err := replaceInstance(state.Async[i]); err != nil {
			return err
		}
	}

	if state.Sync != nil {
		log.Info("replacing the Sync node")
		if err := replaceInstance(state.Sync); err != nil {
			return err
		}
	}

	log.Info("replacing the Primary node")
	if err := replaceInstance(state.Primary); err != nil {
		return err
	}

	log.Info("stopping old postgres jobs")
	d.oldReleaseState["postgres"] = 0
	if err := d.client.PutFormation(&ct.Formation{
		AppID:     d.AppID,
		ReleaseID: d.OldReleaseID,
		Processes: d.oldReleaseState,
	}); err != nil {
		log.Error("error scaling old formation", "err", err)
		return err
	}

	log.Info(fmt.Sprintf("waiting for %d job down events", d.Processes["postgres"]))
	actual := 0
loop:
	for {
		select {
		case event, ok := <-d.jobEvents:
			if !ok {
				return loggedErr("unexpected close of job event stream")
			}
			log.Info("got job event", "job_id", event.JobID, "type", event.Type, "state", event.State)
			if event.State == "down" && event.Type == "postgres" && event.Job.ReleaseID == d.OldReleaseID {
				actual++
				if actual == d.Processes["postgres"] {
					break loop
				}
			}
		case <-time.After(60 * time.Second):
			return loggedErr("timed out waiting for job events")
		}
	}

	// do a one-by-one deploy for the other process types
	return oneByOne(d)
}
