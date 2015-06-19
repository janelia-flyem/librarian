package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/server"
	"github.com/zenazn/goji/graceful"
	"github.com/zenazn/goji/web"
	"github.com/zenazn/goji/web/middleware"
)

const WebHelp = `
<!DOCTYPE html>
<html>

  <head>
	<meta charset='utf-8' />
	<meta http-equiv="X-UA-Compatible" content="chrome=1" />
	<meta name="description" content="Librarian" />

	<title>Librarian Help Page</title>
  </head>

  <body>

	<!-- HEADER -->
	<div id="header_wrap" class="outer">
		<header class="inner">
		  <h2 id="project_tagline">Librarian help page for server currently running on %s</h2>
		</header>
	</div>

	<!-- MAIN CONTENT -->
	<div id="main_content_wrap" class="outer">
	  <section id="main_content" class="inner">
		<p>Librarian is a server for coordinating label assignments among different clients.  It acts
		like a librarian, allowing check-in and check-out of (uuid, label) tuples given a client id.
		The client id is an arbitrary string, e.g., a user name.  All check-ins and check-outs are
		recorded in a human-readable librarian log file.</p>
		
		<h3>HTTP API</h3>

<pre>
 GET  /

	The current help page.

 GET  /checkout/{UUID}

	Returns JSON describing all reserved labels for the given UUID:

	[
		{ "Label": 1, "Client": "katzw" },
		{ "Label": 2019, "Client": "zhaot" },
		...
	]

 PUT  /checkout/{UUID}

 	Reserves a label for the given UUID for a given client id.  The PUT request must be JSON with 
 	"Label" and "Client" fields:

 	{
 		"Label": 34890,
 		"Client": "katzw"
 	}

 	If that label is available for that client, a 200 is returned.  If not, a status 409 (Conflict) is returned.

 GET  /checkout/{UUID}/{Label}

	Returns JSON for any client that has reserved the given label for the UUID:

	{
		"Label": 34890,
		"Client": "katzw"
	}

	If not client has reserved that label, an empty JSON object "{}" is returned.

 PUT  /checkin/{UUID}/{Label}/{Client}

	Checks back in the given label/uuid.  The client id must match the id used to checkout the label.
	If either the client id is incorrect or the given label/uuid was never checked out, a 400 status is returned.

 PUT  /reset/{UUID}

 	Resets all reservations made for the given UUID.  Any checkouts will be deleted.

</pre>

		<h3>Licensing</h3>
		<p><a href="https://github.com/janelia-flyem/dvid">Librarian</a> is released under the
			<a href="http://janelia-flyem.github.com/janelia_farm_license.html">Janelia Farm license</a>, a
			<a href="http://en.wikipedia.org/wiki/BSD_license#3-clause_license_.28.22New_BSD_License.22_or_.22Modified_BSD_License.22.29">
			3-clause BSD license</a>.
		</p>
	  </section>
	</div>

	<!-- FOOTER  -->
	<div id="footer_wrap" class="outer">
	  <footer class="inner">
	  </footer>
	</div>
  </body>
</html>
`

const (
	// WebAPIVersion is the string version of the API.  Once DVID is somewhat stable,
	// this will be "v1/", "v2/", etc.
	WebAPIVersion = ""

	// The relative URL path to our Level 2 REST API
	WebAPIPath = "/" + WebAPIVersion

	// WriteTimeout is the maximum time in seconds DVID will wait to write data down HTTP connection.
	WriteTimeout = 5 * time.Second

	// ReadTimeout is the maximum time in seconds DVID will wait to read data from HTTP connection.
	ReadTimeout = 5 * time.Second
)

type WebMux struct {
	*web.Mux
	sync.Mutex
	routesSetup bool
}

var (
	webMux WebMux
)

func init() {
	webMux.Mux = web.New()
	webMux.Use(middleware.RequestID)
}

// ServeSingleHTTP fulfills one request using the default web Mux.
func ServeSingleHTTP(w http.ResponseWriter, r *http.Request) {
	if !webMux.routesSetup {
		initRoutes()
	}

	// Allow cross-origin resource sharing.
	w.Header().Add("Access-Control-Allow-Origin", "*")

	webMux.ServeHTTP(w, r)
}

func serveHttp(address string) {
	var mode string
	if readonly {
		mode = " (read-only mode)"
	}
	log.Infof("Librarian server listening at %s%s ...\n", address, mode)
	if !webMux.routesSetup {
		initRoutes()
	}

	// Install our handler at the root of the standard net/http default mux.
	// This allows packages like expvar to continue working as expected.  (From goji.go)
	http.Handle("/", webMux)

	graceful.HandleSignals()
	if err := graceful.ListenAndServe(address, http.DefaultServeMux); err != nil {
		log.Fatal(err)
	}
	graceful.Wait()
}

// High-level switchboard
func initRoutes() {
	webMux.Lock()
	defer webMux.Unlock()

	if webMux.routesSetup {
		return
	}

	mainMux := web.New()
	webMux.Handle("/*", mainMux)
	mainMux.Use(middleware.Logger)
	mainMux.Use(middleware.AutomaticOptions)
	mainMux.Use(recoverHandler)
	mainMux.Use(corsHandler)

	mainMux.Put("/checkin/:uuid/:label/:client", putCheckinHandler)
	mainMux.Get("/checkout/:uuid/:label", getCheckoutClientHandler)
	mainMux.Put("/reset/:uuid", resetHandler)
	mainMux.Put("/checkout/:uuid", putCheckoutHandler)
	mainMux.Get("/checkout/:uuid", getCheckoutHandler)
	mainMux.Get("/", helpHandler)
	mainMux.Get("/*", NotFound)

	webMux.routesSetup = true
}

// Middleware that recovers from panics and log issues.
func recoverHandler(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		reqID := middleware.GetReqID(*c)

		defer func() {
			if err := recover(); err != nil {
				buf := make([]byte, 1<<16)
				size := runtime.Stack(buf, false)
				stackTrace := string(buf[0:size])
				message := fmt.Sprintf("Panic detected on request %s:\n%+v\nIP: %v, URL: %s\nStack trace:\n%s\n",
					reqID, err, r.RemoteAddr, r.URL.Path, stackTrace)
				log.Criticalf("%s\n", message)
				http.Error(w, http.StatusText(500), 500)
			}
		}()

		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func NotFound(w http.ResponseWriter, r *http.Request) {
	errorMsg := fmt.Sprintf("Could not find the URL: %s", r.URL.Path)
	log.Infof(errorMsg)
	http.Error(w, errorMsg, http.StatusNotFound)
}

func BadRequest(w http.ResponseWriter, r *http.Request, message string, args ...interface{}) {
	if len(args) > 0 {
		message = fmt.Sprintf(message, args...)
	}
	errorMsg := fmt.Sprintf("%s (%s).", message, r.URL.Path)
	log.Errorf(errorMsg)
	http.Error(w, errorMsg, http.StatusBadRequest)
}

// ---- Middleware -------------

// corsHandler adds CORS support via header
func corsHandler(c *web.C, h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {
		// Allow cross-origin resource sharing.
		w.Header().Add("Access-Control-Allow-Origin", "*")

		h.ServeHTTP(w, r)
	}
	return http.HandlerFunc(fn)
}

func helpHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "Unknown host"
	}

	// Return the embedded help page.
	fmt.Fprintf(w, fmt.Sprintf(WebHelp, hostname))
}

func getCheckoutHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid := c.URLParams["uuid"]

	checkouts, err := getCheckouts(uuid)
	if err != nil {
		BadRequest(w, r, "couldn't get checkouts for uuid %s: %v", uuid, err)
		return
	}

	jsonBytes, err := checkouts.MarshalJSON()
	if err != nil {
		BadRequest(w, r, "error marshaling JSON: %v", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, string(jsonBytes))
}

func putCheckoutHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid := c.URLParams["uuid"]

	client, found := getCheckout(uuid, label)
	if !found {
		BadRequest(w, r, "no checkout for uuid %s, label %d exists", uuid, label)
		return
	}
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		server.BadRequest(w, r, "bad POSTed data for checkout: %v", err)
		return
	}
	var reservation reserveJSON
	if err := json.Unmarshal(data, &reservation); err != nil {
		server.BadRequest(w, r, "bad checkout JSON: %v", err)
		return
	}
	if err := checkout(uuid, reservation.Label, reservation.Client, true); err != nil {
		errorMsg := fmt.Sprintf("could not do checkout: %v (%s).", err, r.URL.Path)
		log.Errorf(errorMsg)
		http.Error(w, errorMsg, http.StatusConflict)
	}
}

func resetHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid := c.URLParams["uuid"]

	if err := reset(uuid, true); err != nil {
		BadRequest(w, r, "unable to reset uuid %s: %v", uuid, err)
	}
}

func getCheckoutClientHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid := c.URLParams["uuid"]
	labelStr := c.URLParams["label"]
	label, err := strconv.ParseUint(labelStr, 10, 64)
	if err != nil {
		BadRequest(w, r, "label %q cannot be parsed as 64-bit unsigned integer: %v", labelStr, err)
		return
	}

	client, found := getCheckout(uuid, label)
	if !found {
		BadRequest(w, r, "no checkout for uuid %s, label %d exists", uuid, label)
		return
	}
	jsonBytes, err := json.Marshal(reserveJSON{label, client})
	if err != nil {
		BadRequest(w, r, "error marshaling JSON: %v", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, string(jsonBytes))
}

func putCheckinHandler(c web.C, w http.ResponseWriter, r *http.Request) {
	uuid := c.URLParams["uuid"]
	client := c.URLParams["client"]
	labelStr := c.URLParams["label"]
	label, err := strconv.ParseUint(labelStr, 10, 64)
	if err != nil {
		BadRequest(w, r, "label %q cannot be parsed as 64-bit unsigned integer: %v", labelStr, err)
		return
	}

	if err := checkin(uuid, label, client, true); err != nil {
		BadRequest(w, r, "unable to checkin: %v", err)
	}
}
