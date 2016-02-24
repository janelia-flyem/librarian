# librarian
Coordinating server for assigning labels to different clients.
Get help by running the librarian and visiting its homepage with a web browser.

## Installing binary

Go to the [Releases tab](https://github.com/janelia-flyem/librarian/releases) and download the appropriate executable.

## Installing from Source

Make sure you have [installed the Go language](http://golang.org).

Set your GOPATH environment variable to where you'd like to keep Go source code.

Add $GOPATH/bin to your PATH environment variable.

    % go get github.com/janelia-flyem/librarian
    % go get github.com/janelia-flyem/go/cron
    % go get github.com/zenazn/goji
    % go install github.com/janelia-flyem/librarian

## Running librarian

    % librarian -help                        # to see options
    % librarian /path/to/librarian.log       # starts server on port 8000 (default) storing record of requests in log file
