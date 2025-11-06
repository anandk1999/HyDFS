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
	conn            *net.UDPConn
	dropRate        float32
	messageQueue    chan ReceivedMessage
	priorityQueue   chan ReceivedMessage // High-priority queue for membership messages
	handlers        map[MessageType]func(Message, *net.UDPAddr)
	closed          chan bool
	mutex           sync.RWMutex
	sendRateLimiter chan struct{} // Rate limiting for sends
}

// ReceivedMessage keeps the decoded payload along with who sent it.
type ReceivedMessage struct {
	Message Message
	From    *net.UDPAddr
}

// NewNetworkLayer sets up queues and sane defaults.
func NewNetworkLayer() *NetworkLayer {
	// Rate limiter: allow up to 100 concurrent sends
	rateLimiter := make(chan struct{}, 100)
	for i := 0; i < 100; i++ {
		rateLimiter <- struct{}{}
	}

	return &NetworkLayer{
		dropRate:        0.0,
		messageQueue:    make(chan ReceivedMessage, 5000), // Increased buffer for DFS traffic
		priorityQueue:   make(chan ReceivedMessage, 1000), // Priority queue for membership
		handlers:        make(map[MessageType]func(Message, *net.UDPAddr)),
		closed:          make(chan bool),
		sendRateLimiter: rateLimiter,
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
	n.conn.SetReadBuffer(2097152)  // 2MB buffer (increased for DFS traffic)
	n.conn.SetWriteBuffer(2097152) // 2MB write buffer

	go n.receiveLoop()
	go n.processQueue()
	go n.processPriorityQueue() // Separate goroutine for priority messages

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

			// Route membership protocol messages to priority queue
			isPriorityMsg := msg.Type == Ping || msg.Type == Ack ||
				msg.Type == IndirectPing || msg.Type == IndirectAck ||
				msg.Type == Join || msg.Type == JoinResponse ||
				msg.Type == AliveMsg || msg.Type == Suspect ||
				msg.Type == Leave || msg.Type == Confirm

			if isPriorityMsg {
				select {
				case n.priorityQueue <- ReceivedMessage{Message: msg, From: addr}:
				default:
					// Priority queue full, log but don't drop membership messages
					log.Println("Priority queue full, using regular queue for membership message")
					select {
					case n.messageQueue <- ReceivedMessage{Message: msg, From: addr}:
					default:
						log.Println("All queues full, dropping message")
					}
				}
			} else {
				select {
				case n.messageQueue <- ReceivedMessage{Message: msg, From: addr}:
				default:
					log.Println("Message queue full, dropping DFS message")
				}
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

// processPriorityQueue handles high-priority membership protocol messages
func (n *NetworkLayer) processPriorityQueue() {
	for {
		select {
		case <-n.closed:
			return
		case received := <-n.priorityQueue:
			n.mutex.RLock()
			handler, exists := n.handlers[received.Message.Type]
			n.mutex.RUnlock()

			if exists {
				handler(received.Message, received.From)
			}
		}
	}
}

// Send encodes the message and blasts it over UDP with rate limiting.
func (n *NetworkLayer) Send(msg Message, target string) error {
	msg.Timestamp = time.Now().Unix()

	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("Error marshaling message: %v", err)
		return err
	}

	// Rate limiting: wait for token with timeout
	select {
	case <-n.sendRateLimiter:
		// Got token, proceed with send
		defer func() {
			// Return token after a small delay to prevent bursts
			time.AfterFunc(10*time.Millisecond, func() {
				select {
				case n.sendRateLimiter <- struct{}{}:
				default:
				}
			})
		}()
	case <-time.After(100 * time.Millisecond):
		// Timeout waiting for rate limiter - send anyway but log
		log.Printf("Rate limiter timeout for message type %v to %s", msg.Type, target)
	}

	addr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		log.Printf("Error resolving address %s: %v", target, err)
		return err
	}

	// Set write deadline to prevent blocking forever
	n.conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
	_, err = n.conn.WriteToUDP(data, addr)
	n.conn.SetWriteDeadline(time.Time{}) // Clear deadline

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
