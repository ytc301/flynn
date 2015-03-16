package snapshot

import (
	"github.com/flynn/flynn/logaggregator/ring"
	"github.com/flynn/flynn/pkg/syslog/rfc5424"
	"github.com/gogo/protobuf/proto"
)

//go:generate protoc --gogo_out=. snapshot.proto

func Take(buffers map[string]*ring.Buffer) ([]byte, error) {
	ss := &Snapshot{
		Buffers: make([]*SnapshotBuffer, 0, len(buffers)),
	}

	for key, buf := range buffers {
		msgs := buf.ReadAll()

		sbuf := &SnapshotBuffer{
			Key:      &key,
			Messages: make([][]byte, len(msgs)),
		}

		for i, msg := range msgs {
			sbuf.Messages[i] = msg.Bytes()
		}

		ss.Buffers = append(ss.Buffers, sbuf)
	}

	return proto.Marshal(ss)
}

func Load(data []byte) (map[string]*ring.Buffer, error) {
	ss := &Snapshot{}
	if err := proto.Unmarshal(data, ss); err != nil {
		return nil, err
	}

	buffers := make(map[string]*ring.Buffer, len(ss.Buffers))
	for _, sbuf := range ss.Buffers {
		rbuf := ring.NewBuffer()

		for _, msg := range sbuf.Messages {
			smsg, err := rfc5424.Parse(msg)
			if err != nil {
				return nil, err
			}

			rbuf.Add(smsg)
		}

		buffers[*sbuf.Key] = rbuf
	}

	return buffers, nil
}
