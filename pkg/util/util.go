package util


// Find takes a slice and looks for an element in it. If found it will
// return it's key, otherwise it will return -1 and a bool of false.
func Find(slice []string, val string) (int, bool) {
	for i, item := range slice {
		if item == val {
			return i, true
		}
	}
	return -1, false
}

// Unwrap pops frame off front of message and returns it as 'head'. If next
// frame is empty, pops that empty frame. Return remaining frames of message as
// 'tail'
func Unwrap(msg []string) (head string, tail []string) {
	head = msg[0]
	if len(msg) > 1 && msg[1] == "" {
		tail = msg[2:]
	} else {
		tail = msg[1:]
	}
	return
}


func PopStr(ss []string) (s string, ss2 []string) {
	s = ss[0]
	ss2 = ss[1:]
	return
}

func PopMsg(msgs [][]string) (msg []string, msgs2 [][]string) {
	msg = msgs[0]
	msgs2 = msgs[1:]
	return
}

func Btou(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}