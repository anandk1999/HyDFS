package utils

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"sync"
	"time"
)

// GetLocalIP returns the local IP address of the machine.
func GetLocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}

// NetworkLayer wraps our UDP socket plus message dispatch helpers.
type NetworkLayer struct {
	conn         *net.UDPConn
	dropRate     float32
	messageQueue chan ReceivedMessage
	handlers     map[MessageType]func(Message, *net.UDPAddr)
	closed       chan bool
	mutex        sync.RWMutex
}

// ReceivedMessage keeps the decoded payload along with who sent it.
type ReceivedMessage struct {
	Message Message
	From    *net.UDPAddr
}

// NewNetworkLayer sets up queues and sane defaults.
func NewNetworkLayer() *NetworkLayer {
	return &NetworkLayer{
		dropRate:     0.0,
		messageQueue: make(chan ReceivedMessage, 1000),
		handlers:     make(map[MessageType]func(Message, *net.UDPAddr)),
		closed:       make(chan bool),
	}
}

// Start binds the UDP socket and kicks off the receiver goroutines.
func (n *NetworkLayer) Start(port int) error {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Printf("Error in trying to start in network.go: %v", err)
		return err
	}
	log.Printf("Starting UDP server on %s", addr.String())

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	n.conn = conn
	n.conn.SetReadBuffer(1048576) // 1MB buffer

	go n.receiveLoop()
	go n.processQueue()

	return nil
}

// receiveLoop pulls bytes off the socket and shoves them onto the queue.
func (n *NetworkLayer) receiveLoop() {
	buffer := make([]byte, 65536)

	for {
		select {
		case <-n.closed:
			return
		default:
			n.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			bytesRead, addr, err := n.conn.ReadFromUDP(buffer)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				if err.Error() != "use of closed network connection" {
					log.Printf("Error reading UDP: %v", err)
				}
				continue
			}

			// Simulate message drop at receiver
			n.mutex.RLock()
			dropRate := n.dropRate
			n.mutex.RUnlock()

			if rand.Float32() < dropRate {
				continue
			}

			var msg Message
			if err := json.Unmarshal(buffer[:bytesRead], &msg); err != nil {
				log.Printf("Error unmarshaling message: %v", err)
				continue
			}

			select {
			case n.messageQueue <- ReceivedMessage{Message: msg, From: addr}:
			default:
				log.Println("Message queue full, dropping message")
			}
		}
	}
}

// processQueue fans messages out to whichever handler registered for them.
func (n *NetworkLayer) processQueue() {
	for {
		select {
		case <-n.closed:
			return
		case received := <-n.messageQueue:
			n.mutex.RLock()
			handler, exists := n.handlers[received.Message.Type]
			n.mutex.RUnlock()

			if exists {
				handler(received.Message, received.From)
			}
		}
	}
}

// Send encodes the message and blasts it over UDP.
func (n *NetworkLayer) Send(msg Message, target string) error {
	msg.Timestamp = time.Now().Unix()

	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Error marshaling message: %v", err)
		return err
	}

	addr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		log.Printf("Error resolving address %s: %v", target, err)
		return err
	}

	_, err = n.conn.WriteToUDP(data, addr)
	if err != nil {
		log.Printf("Error sending UDP message to %s: %v", target, err)
	}
	return err
}

// RegisterHandler lets detectors subscribe to a message type.
func (n *NetworkLayer) RegisterHandler(msgType MessageType, handler func(Message, *net.UDPAddr)) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.handlers[msgType] = handler
}

// SetDropRate toggles how often we pretend packets vanish.
func (n *NetworkLayer) SetDropRate(rate float32) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.dropRate = rate
}

// Stop closes the socket and stops the goroutines.
func (n *NetworkLayer) Stop() {
	close(n.closed)
	if n.conn != nil {
		n.conn.Close()
	}
}
