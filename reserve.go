package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

const (
	logFmt = "%s %s %d %s"
)

type opType uint8

func (op opType) String() string {
	switch op {
	case CheckoutOp:
		return "checkout"
	case CheckinOp:
		return "checkin"
	case ResetOp:
		return "reset"
	default:
		return "unknown-op"
	}
}

func opTypeFromString(s string) opType {
	switch s {
	case "checkout":
		return CheckoutOp
	case "checkin":
		return CheckinOp
	case "reset":
		return ResetOp
	default:
		return UnknownOp
	}
}

const (
	UnknownOp opType = iota
	CheckoutOp
	CheckinOp
	ResetOp
)

type libraryOp struct {
	t      time.Time
	op     opType
	uuid   string
	label  uint64
	client string
}

type reserveJSON struct {
	Label  uint64
	Client string
}

type checkoutsT map[uint64]string

func (c checkoutsT) MarshalJSON() ([]byte, error) {
	reserves := make([]reserveJSON, len(c))
	i := 0
	for label, client := range c {
		reserves[i] = reserveJSON{label, client}
		i++
	}
	return json.Marshal(reserves)
}

// map of UUID -> checkouts
type libraryT struct {
	sync.RWMutex

	vchk  map[string]checkoutsT
	fname string
	w     *bufio.Writer // Append-only log writer
}

var (
	library libraryT
)

func (lib *libraryT) write(op *libraryOp) error {
	timeBytes, err := time.Now().MarshalText()
	if err != nil {
		return err
	}
	line := fmt.Sprintf("%s %s %s %d %s\n", string(timeBytes), op.uuid, op.op, op.label, op.client)
	if _, err := lib.w.WriteString(line); err != nil {
		return err
	}
	if err := lib.w.Flush(); err != nil {
		return err
	}
	return nil
}

// This is the only time we read from log file, then rest of time we write.
func initLibrary(fname string) error {
	library.fname = fname
	library.vchk = make(map[string]checkoutsT, 100)

	// Read-only mode
	f, err := os.OpenFile(fname, os.O_CREATE|os.O_RDONLY, 0664)
	if err != nil {
		return fmt.Errorf("cannot create/open librarian log file: %v", err)
	}
	r := bufio.NewReader(f)

	// Load every entry in, populating our library of reserved labels.
	modifyLog := false
	for {
		line, err := r.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		op, err := parseLogLine(line)
		if err != nil {
			return err
		}
		switch op.op {
		case CheckoutOp:
			checkout(op.uuid, op.label, op.client, modifyLog)
		case CheckinOp:
			checkin(op.uuid, op.label, op.client, modifyLog)
		case ResetOp:
			reset(op.uuid, modifyLog)
		default:
			return fmt.Errorf("bad log op found in initLibrary!  Should not happen.")
		}
	}

	// After full read, open the file os.O_APPEND|os.O_CREATE rather than use os.Create.
	// Append is almost always more efficient than O_RDRW on most modern file systems.
	w, err := os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0664)
	if err != nil {
		return fmt.Errorf("cannot open librarian log file: %v", err)
	}
	library.w = bufio.NewWriter(w)
	return nil
}

func parseLogLine(line string) (*libraryOp, error) {
	var timeStr, uuid, opStr, client string
	var label uint64
	n, err := fmt.Sscanf(line, "%s %s %s %d %s", &timeStr, &uuid, &opStr, &label, &client)
	if err != nil {
		return nil, fmt.Errorf("could not parse log line %q: %v", line, err)
	}
	if n != 5 {
		return nil, fmt.Errorf("could not parse log line %q", line)
	}
	var t time.Time
	if err := t.UnmarshalText([]byte(timeStr)); err != nil {
		return nil, err
	}
	op := &libraryOp{
		t:      t,
		op:     opTypeFromString(opStr),
		uuid:   uuid,
		label:  label,
		client: client,
	}
	return op, nil
}

// Writes JSON of history for a UUID into a writer.
func writeHx(uuid string, w io.Writer) error {
	// Read-only mode
	f, err := os.OpenFile(library.fname, os.O_RDONLY, 0664)
	if err != nil {
		return fmt.Errorf("cannot open librarian log file: %v", err)
	}
	defer f.Close()
	r := bufio.NewReader(f)

	// Load every entry in, populating our library of reserved labels.
	fmt.Fprintf(w, "[\n")
	first := true
	for {
		line, err := r.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		op, err := parseLogLine(line)
		if err != nil {
			return err
		}
		if op.uuid == uuid {
			tbytes, err := op.t.MarshalText()
			if err != nil {
				return err
			}
			if first {
				fmt.Fprintf(w, "\n  {")
			} else {
				fmt.Fprintf(w, ",\n  {")
			}
			fmt.Fprintf(w, `"Time":%q, "Op":%q`, string(tbytes), op.op)
			switch op.op {
			case CheckoutOp, CheckinOp:
				fmt.Fprintf(w, `, "Label":%d, "Client":%q`, op.label, op.client)
			}
			fmt.Fprintf(w, "}")
			first = false
		}
	}
	fmt.Fprintf(w, "]\n")
	return nil
}

func checkout(uuid string, label uint64, clientid string, modifyLog bool) error {
	library.Lock()
	defer library.Unlock()

	// Append to in-memory map
	checkouts, found := library.vchk[uuid]
	if found {
		client, labelUsed := checkouts[label]
		if labelUsed {
			if client != clientid {
				return fmt.Errorf("uuid %s, label %d - already checked out by %s", uuid, label, client)
			}
		} else {
			checkouts[label] = clientid
		}
	} else {
		checkouts = make(map[uint64]string, 100)
		checkouts[label] = clientid
		library.vchk[uuid] = checkouts
	}

	// Append to log
	if modifyLog {
		op := &libraryOp{
			op:     CheckoutOp,
			uuid:   uuid,
			label:  label,
			client: clientid,
		}
		library.write(op)
	}
	return nil
}

func getUUIDs() []string {
	library.RLock()
	defer library.RUnlock()

	uuids := make([]string, len(library.vchk))
	i := 0
	for uuid := range library.vchk {
		uuids[i] = uuid
		i++
	}
	return uuids
}

func getUUIDsJSON() (string, error) {
	uuids := getUUIDs()
	jsonBytes, err := json.Marshal(uuids)
	return string(jsonBytes), err
}

func getCheckout(uuid string, label uint64) (client string, found bool) {
	library.RLock()
	defer library.RUnlock()

	checkouts, uuidFound := library.vchk[uuid]
	if uuidFound {
		client, found = checkouts[label]
	} else {
		found = false
	}
	return
}

func getCheckouts(uuid string) (checkouts checkoutsT, found bool) {
	library.RLock()
	defer library.RUnlock()

	checkouts, found = library.vchk[uuid]
	return
}

func checkin(uuid string, label uint64, clientid string, modifyLog bool) error {
	library.Lock()
	defer library.Unlock()

	// Remove from in-memory map
	checkouts, found := library.vchk[uuid]
	if found {
		client, labelUsed := checkouts[label]
		if labelUsed {
			if client != clientid {
				return fmt.Errorf("uuid %s, label %d checked out to %s, not %s so cannot checkin", uuid, label, client, clientid)
			}
			delete(library.vchk[uuid], label)
		} else {
			return fmt.Errorf("uuid %s, label %d has not been checked out so can't be checked in by %s", uuid, label, client)
		}
	} else {
		return fmt.Errorf("uuid %s has no active checkout so can't checkin label %d, client %s", uuid, label, clientid)
	}

	// Append to log
	if modifyLog {
		op := &libraryOp{
			op:     CheckinOp,
			uuid:   uuid,
			label:  label,
			client: clientid,
		}
		library.write(op)
	}
	return nil
}

func reset(uuid string, modifyLog bool) error {
	library.Lock()
	defer library.Unlock()

	// Delete all in-memory checkouts for this uuid
	delete(library.vchk, uuid)

	// Append to log
	if modifyLog {
		op := &libraryOp{
			op:     ResetOp,
			uuid:   uuid,
			client: "n/a",
		}
		library.write(op)
	}
	return nil
}
