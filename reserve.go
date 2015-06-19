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
	vchk map[string]checkoutsT
	w    *bufio.Writer // Append-only log writer
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
	return nil
}

// This is the only time we read from log file, then rest of time we write.
func initLibrary(fname string) error {
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
	if n != 5 {
		return nil, fmt.Errorf("could not parse log line %q", line)
	}

	op := &libraryT{
		op:     opTypeFromString(opStr),
		uuid:   uuid,
		label:  label,
		client: client,
	}
	return op, nil
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
			client[label] = clientid
		}
	} else {
		checkouts = make(map[uint64]string, 100)
		checkouts[label] = clientid
		library.vchk[uuid] = checkouts
	}

	// Append to log
	if modifyLog {
		op := libraryOp{
			op:     CheckoutOp,
			uuid:   uuid,
			label:  label,
			client: clientid,
		}
		library.write(op)
	}
}

func getCheckout(uuid string, label uint64) (client string, found bool) {
	library.RLock()
	defer library.RUnlock()

	checkouts, uuidFound = library.vchk[uuid]
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
			delete(library.vchk[uuid], label)
		} else {
			return fmt.Errorf("uuid %s, label %d has not been checked out so can't be checked in by %s", uuid, label, client)
		}
	} else {
		return fmt.Errorf("uuid %s has no active checkout so can't checkin label %d, client %s", uuid, label, clientid)
	}

	// Append to log
	if modifyLog {
		op := libraryOp{
			op:     CheckinOp,
			uuid:   uuid,
			label:  label,
			client: clientid,
		}
		library.write(op)
	}
}

func reset(uuid string, modifyLog bool) error {
	library.Lock()
	defer library.Unlock()

	// Delete all in-memory checkouts for this uuid
	delete(library.vchk, uuid)

	// Append to log
	if modifyLog {
		op := libraryOp{
			op:     ResetOp,
			uuid:   uuid,
			client: "n/a",
		}
		library.write(op)
	}
}
