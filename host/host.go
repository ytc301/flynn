package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-docopt"
	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/technoweenie/grohl"
	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/host/cli"
	"github.com/flynn/flynn/host/config"
	"github.com/flynn/flynn/host/logmux"
	"github.com/flynn/flynn/host/sampi"
	"github.com/flynn/flynn/host/types"
	"github.com/flynn/flynn/host/volume"
	"github.com/flynn/flynn/host/volume/manager"
	zfsVolume "github.com/flynn/flynn/host/volume/zfs"
	"github.com/flynn/flynn/pkg/attempt"
	"github.com/flynn/flynn/pkg/cluster"
	"github.com/flynn/flynn/pkg/shutdown"
	"github.com/flynn/flynn/pkg/version"
)

// discoverdAttempts is the attempt strategy that is used to connect to discoverd.
var discoverdAttempts = attempt.Strategy{
	Min:   5,
	Total: 10 * time.Minute,
	Delay: 200 * time.Millisecond,
}

const configFile = "/etc/flynn/host.json"

func init() {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)

	cli.Register("daemon", runDaemon, `
usage: flynn-host daemon [options] [--meta=<KEY=VAL>...]

options:
  --external=IP          external IP of host
  --manifest=PATH        path to manifest file [default: /etc/flynn/host-manifest.json]
  --state=PATH           path to state file [default: /var/lib/flynn/host-state.bolt]
  --id=ID                host id
  --force                kill all containers booted by flynn-host before starting
  --legacy-volpath=PATH  directory to create legacy volumes in [default: /var/lib/flynn/host-volumes]
  --volpath=PATH         directory to create volumes in [default: /var/lib/flynn/volumes]
  --backend=BACKEND      runner backend [default: libvirt-lxc]
  --meta=<KEY=VAL>...    key=value pair to add as metadata
  --bind=IP              bind containers to IP
  --flynn-init=PATH      path to flynn-init binary [default: /usr/local/bin/flynn-init]
	`)
}

func main() {
	defer shutdown.Exit()

	usage := `usage: flynn-host [-h|--help] [--version] <command> [<args>...]

Options:
  -h, --help                 Show this message
  --version                  Show current version

Commands:
  help                       Show usage for a specific command
  init                       Create cluster configuration for daemon
  daemon                     Start the daemon
  update                     Update Flynn components
  download                   Download container images
  bootstrap                  Bootstrap layer 1
  inspect                    Get low-level information about a job
  log                        Get the logs of a job
  ps                         List jobs
  stop                       Stop running jobs
  destroy-volumes            Destroys the local volume database
  collect-debug-info         Collect debug information into an anonymous gist or tarball
  version                    Show current version

See 'flynn-host help <command>' for more information on a specific command.
`

	args, _ := docopt.Parse(usage, nil, true, version.String(), true)
	cmd := args.String["<command>"]
	cmdArgs := args.All["<args>"].([]string)

	if cmd == "help" {
		if len(cmdArgs) == 0 { // `flynn help`
			fmt.Println(usage)
			return
		} else { // `flynn help <command>`
			cmd = cmdArgs[0]
			cmdArgs = []string{"--help"}
		}
	}

	if cmd == "daemon" {
		// merge in args and env from config file, if available
		var c *config.Config
		var err error
		if n := os.Getenv("FLYNN_HOST_CONFIG"); n != "" {
			c, err = config.Open(n)
			if err != nil {
				log.Fatalf("error opening config file %s: %s", n, err)
			}
		} else {
			c, err = config.Open(configFile)
			if err != nil && !os.IsNotExist(err) {
				log.Fatalf("error opening config file %s: %s", configFile, err)
			}
			if c == nil {
				c = &config.Config{}
			}
		}
		cmdArgs = append(cmdArgs, c.Args...)
		for k, v := range c.Env {
			os.Setenv(k, v)
		}
	}

	if err := cli.Run(cmd, cmdArgs); err != nil {
		if err == cli.ErrInvalidCommand {
			fmt.Printf("ERROR: %q is not a valid command\n\n", cmd)
			fmt.Println(usage)
			shutdown.ExitWithCode(1)
		}
		shutdown.Fatal(err)
	}
}

func runDaemon(args *docopt.Args) {
	hostname, _ := os.Hostname()
	externalAddr := args.String["--external"]
	bindAddr := args.String["--bind"]
	manifestFile := args.String["--manifest"]
	stateFile := args.String["--state"]
	hostID := args.String["--id"]
	force := args.Bool["--force"]
	legacyVolPath := args.String["--legacyvolpath"]
	volPath := args.String["--volpath"]
	backendName := args.String["--backend"]
	flynnInit := args.String["--flynn-init"]
	metadata := args.All["--meta"].([]string)

	grohl.AddContext("app", "host")
	grohl.Log(grohl.Data{"at": "start"})
	g := grohl.NewContext(grohl.Data{"fn": "main"})

	if hostID == "" {
		hostID = strings.Replace(hostname, "-", "", -1)
	}
	if strings.Contains(hostID, "-") {
		shutdown.Fatal("host id must not contain dashes")
	}
	if externalAddr == "" {
		var err error
		externalAddr, err = config.DefaultExternalIP()
		if err != nil {
			shutdown.Fatal(err)
		}
	}

	state := NewState(hostID, stateFile)
	var backend Backend
	var err error

	// create volume manager
	vman, err := volumemanager.New(
		filepath.Join(volPath, "volumes.bolt"),
		func() (volume.Provider, error) {
			return zfsVolume.NewProvider(&zfsVolume.ProviderConfig{
				DatasetName: "flynn-default",
				Make: &zfsVolume.MakeDev{
					BackingFilename: filepath.Join(volPath, "zfs/vdev/flynn-default-zpool.vdev"),
					Size:            100000000000, // Provision a 100GB sparse file
				},
				WorkingDir: filepath.Join(volPath, "zfs"),
			})
		},
	)
	if err != nil {
		shutdown.Fatal(err)
	}

	mux := logmux.New(1000)
	shutdown.BeforeExit(func() { mux.Close() })

	switch backendName {
	case "libvirt-lxc":
		backend, err = NewLibvirtLXCBackend(state, vman, legacyVolPath, "/tmp/flynn-host-logs", flynnInit, mux)
	default:
		log.Fatalf("unknown backend %q", backendName)
	}
	if err != nil {
		shutdown.Fatal(err)
	}

	resurrectLayer1, err := state.Restore(backend)
	if err != nil {
		shutdown.Fatal(err)
	}

	shutdown.BeforeExit(func() { backend.Cleanup() })
	shutdown.BeforeExit(func() {
		if err := state.MarkForResurrection(); err != nil {
			log.Print("error marking for resurrection", err)
		}
	})

	if force {
		if err := backend.Cleanup(); err != nil {
			shutdown.Fatal(err)
		}
	}

	runner := &manifestRunner{
		env:          parseEnviron(),
		externalAddr: externalAddr,
		bindAddr:     bindAddr,
		backend:      backend,
		state:        state,
		vman:         vman,
	}

	// connect to discoverd
	discURL := os.Getenv("DISCOVERD")
	var disc *discoverd.Client
	if manifestFile != "" {
		var r io.Reader
		var f *os.File
		if manifestFile == "-" {
			r = os.Stdin
		} else {
			f, err = os.Open(manifestFile)
			if err != nil {
				shutdown.Fatal(err)
			}
			r = f
		}
		services, err := runner.runManifest(r)
		if err != nil {
			shutdown.Fatal(err)
		}
		if f != nil {
			f.Close()
		}

		if d, ok := services["discoverd"]; ok {
			discURL = fmt.Sprintf("http://%s:%d", d.ExternalIP, d.TCPPorts[0])
			disc = discoverd.NewClientWithURL(discURL)
			if err := discoverdAttempts.Run(disc.Ping); err != nil {
				shutdown.Fatal(err)
			}
		}
	}

	if discURL == "" && externalAddr != "" {
		discURL = fmt.Sprintf("http://%s:1111", externalAddr)
	}
	// HACK: use env as global for discoverd connection in sampic
	os.Setenv("DISCOVERD", discURL)
	if disc == nil {
		disc = discoverd.NewClientWithURL(discURL)
		if err := disc.Ping(); err != nil {
			shutdown.Fatal(err)
		}
	}
	hb, err := disc.AddServiceAndRegisterInstance("flynn-host", &discoverd.Instance{
		Addr: externalAddr + ":1113",
		Meta: map[string]string{"id": hostID},
	})
	if err != nil {
		shutdown.Fatal(err)
	}
	shutdown.BeforeExit(func() { hb.Close() })

	if err := mux.Connect(disc, "flynn-logaggregator"); err != nil {
		shutdown.Fatal(err)
	}

	cluster, err := cluster.NewClient()
	if err != nil {
		shutdown.Fatal(err)
	}

	router, err := serveHTTP(
		&Host{state: state, backend: backend},
		&attachHandler{state: state, backend: backend},
		cluster,
		vman,
	)
	if err != nil {
		shutdown.Fatal(err)
	}

	sampiAPI := sampi.NewHTTPAPI(sampi.NewCluster())
	leaders := make(chan *discoverd.Instance)
	leaderStream, err := disc.Service("flynn-host").Leaders(leaders)
	if err != nil {
		shutdown.Fatal(err)
	}
	promote := func() {
		g.Log(grohl.Data{"at": "sampi_leader"})
		sampiAPI.RegisterRoutes(router)
		leaderStream.Close()
	}
	leader := <-leaders
	if leader.Addr == hb.Addr() {
		promote()
		// TODO: handle demotion
	} else {
		go func() {
			for leader := range leaders {
				if leader.Addr == hb.Addr() {
					promote()
					return
				}
			}
			// TODO: handle discoverd disconnection
		}()
	}

	g.Log(grohl.Data{"at": "sampi_connected"})

	events := state.AddListener("all")
	go syncScheduler(cluster, hostID, events)

	h := &host.Host{ID: hostID, Metadata: make(map[string]string)}
	for _, s := range metadata {
		kv := strings.SplitN(s, "=", 2)
		h.Metadata[kv[0]] = kv[1]
	}

	if err := resurrectLayer1(); err != nil {
		shutdown.Fatal(err)
	}

	for {
		newLeader := cluster.NewLeaderSignal()

		h.Jobs = state.ClusterJobs()
		jobs := make(chan *host.Job)
		jobStream, err := cluster.RegisterHost(h, jobs)
		if err != nil {
			shutdown.Fatal(err)
		}
		shutdown.BeforeExit(func() {
			// close the connection that registers use with the cluster
			// during shutdown; this unregisters us immediately.
			jobStream.Close()
		})
		g.Log(grohl.Data{"at": "host_registered"})
		for job := range jobs {
			if externalAddr != "" {
				if job.Config.Env == nil {
					job.Config.Env = make(map[string]string)
				}
				job.Config.Env["EXTERNAL_IP"] = externalAddr
				job.Config.Env["DISCOVERD"] = discURL
			}
			if err := backend.Run(job, nil); err != nil {
				state.SetStatusFailed(job.ID, err)
			}
		}
		g.Log(grohl.Data{"at": "sampi_disconnected", "err": jobStream.Err})

		// if the process is shutting down, just block
		if shutdown.IsActive() {
			<-make(chan struct{})
		}

		<-newLeader
	}
}

func syncScheduler(scheduler *cluster.Client, hostID string, events <-chan host.Event) {
	for event := range events {
		if event.Event != "stop" {
			continue
		}
		grohl.Log(grohl.Data{"fn": "scheduler_event", "at": "remove_job", "job.id": event.JobID})
		if err := scheduler.RemoveJob(hostID, event.JobID); err != nil {
			grohl.Log(grohl.Data{"fn": "scheduler_event", "at": "remove_job", "status": "error", "err": err, "job.id": event.JobID})
		}
	}
}
