// Package fairymqgo
// // // // // // // // // // // // // // // // // // // // // // // // // //
// fairyMQ GO Native Client Module
// // // // // // // // // // // // // // // // // // // // // // // // // //
// Originally authored by Alex Gaetano Padula
// Copyright (C) fairyMQ
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
// // // // // // // // // // // // // // // // // // // // // // // // // //

package fairymqgo

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	"time"
)

// Client is the fairyMQ client structure
type Client struct {
	Host            string       // i.e 0.0.0.0:5991
	PublicKey       string       //i.e testing.public.pem
	UDPAddr         *net.UDPAddr // address of UDP end point
	UDPConn         *net.UDPConn // UDP connection
	ParsedPublicKey any          // Parsed public key
}

// Configure configures public key amongst other things.  Must run before anything else
func (client *Client) Configure() error {
	var err error

	// Resolve UDP address
	client.UDPAddr, err = net.ResolveUDPAddr("udp", client.Host)
	if err != nil {
		return errors.New(fmt.Sprintf("could not configure client. %s", err.Error()))
	}

	// Dial address
	client.UDPConn, err = net.DialUDP("udp", nil, client.UDPAddr)
	if err != nil {
		return errors.New(fmt.Sprintf("could not configure client. %s", err.Error()))
	}

	publicKeyPEM, err := os.ReadFile(client.PublicKey)
	if err != nil {
		return errors.New(fmt.Sprintf("could not configure client. %s", err.Error()))
	}

	publicKeyBlock, _ := pem.Decode(publicKeyPEM)

	client.ParsedPublicKey, err = x509.ParsePKIXPublicKey(publicKeyBlock.Bytes)
	if err != nil {
		return errors.New(fmt.Sprintf("could not configure client. %s", err.Error()))
	}

	return nil
}

// Enqueue enqueues a new message into queue
func (client *Client) Enqueue(data []byte) error {

	var err error
	attempts := 0 // Max attempts to reach server is 10
	// Mark the creation of message
	timestamp := time.Now().UnixMicro()

	plaintext := append([]byte(fmt.Sprintf("ENQUEUE\r\n%d\r\n", timestamp)), data...)
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, client.ParsedPublicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = client.UDPConn.Write(ciphertext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = client.UDPConn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	// Read from server
	res, err := bufio.NewReader(client.UDPConn).ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			attempts += 1

			if attempts < 10 {
				goto try
			} else {
				return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
			}
		} else {
			return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
		}
	}

	if bytes.HasPrefix(res, []byte("ACK")) {
		return nil
	} else {
		return errors.New("invalid response.  expecting ack.")
	}

	return nil
}

// EnqueueWithKey enqueues a new message into queue with a provided key.  Keys are not unique
func (client *Client) EnqueueWithKey(data []byte, messageKey string) error {

	attempts := 0 // Max attempts to reach server is 10

	// Mark the creation of message
	timestamp := time.Now().UnixMicro()

	plaintext := append([]byte(fmt.Sprintf("ENQUEUE %s\r\n%d\r\n", messageKey, timestamp)), data...)
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, client.ParsedPublicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = client.UDPConn.Write(ciphertext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = client.UDPConn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	// Read from server
	res, err := bufio.NewReader(client.UDPConn).ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			attempts += 1

			if attempts < 10 {
				goto try
			} else {
				return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
			}
		} else {
			return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
		}
	}

	if bytes.HasPrefix(res, []byte("ACK")) {
		return nil
	} else {
		return errors.New("invalid response.  expecting ack.")
	}

	return nil
}

// Length get length of queue
func (client *Client) Length() ([]byte, error) {

	attempts := 0 // Max attempts to reach server is 10

	plaintext := []byte(fmt.Sprintf("LENGTH\r\n"))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, client.ParsedPublicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get length of queue. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = client.UDPConn.Write(ciphertext)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get length of queue. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = client.UDPConn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get length of queue. %s", err.Error()))
	}

	// Read from server
	res, err := bufio.NewReader(client.UDPConn).ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			attempts += 1

			if attempts < 10 {
				goto try
			} else {
				return nil, errors.New(fmt.Sprintf("could not get length of queue. %s", err.Error()))
			}
		} else {
			return nil, errors.New(fmt.Sprintf("could not get length of queue. %s", err.Error()))
		}
	}

	return res, nil
}

// FirstIn first message up in queue
func (client *Client) FirstIn() ([]byte, error) {

	attempts := 0 // Max attempts to reach server is 10

	plaintext := []byte(fmt.Sprintf("FIRST IN\r\n"))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, client.ParsedPublicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get first message in queue. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = client.UDPConn.Write(ciphertext)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get first message in queue. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = client.UDPConn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get first message in queue. %s", err.Error()))
	}

	// Read from server
	res, err := bufio.NewReader(client.UDPConn).ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			attempts += 1

			if attempts < 10 {
				goto try
			} else {
				return nil, errors.New(fmt.Sprintf("could not get first message in queue. %s", err.Error()))
			}
		} else {
			return nil, errors.New(fmt.Sprintf("could not get first message in queue. %s", err.Error()))
		}
	}

	return res, nil
}

func (client *Client) GetAllMessagesByKey(key string) ([][]byte, error) {

	attempts := 0 // Max attempts to reach server is 10

	plaintext := []byte(fmt.Sprintf("MSGS WITH KEY %s\r\n", key))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, client.ParsedPublicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get first message in queue. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = client.UDPConn.Write(ciphertext)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get first message in queue. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = client.UDPConn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get first message in queue. %s", err.Error()))
	}

	// Read from server
	res, err := bufio.NewReader(client.UDPConn).ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			attempts += 1

			if attempts < 10 {
				goto try
			} else {
				return nil, errors.New(fmt.Sprintf("could not get first message in queue. %s", err.Error()))
			}
		} else {
			return nil, errors.New(fmt.Sprintf("could not get first message in queue. %s", err.Error()))
		}
	}

	return bytes.Split(res, []byte("\r\n\r\n")), nil
}

// ExpireMessages sets whether queue expires messages or not.  Default is 7200 seconds
func (client *Client) ExpireMessages(enabled bool) error {

	bI := 0

	if enabled {
		bI = 1
	}

	attempts := 0 // Max attempts to reach server is 10

	plaintext := []byte(fmt.Sprintf("EXP MSGS %d\r\n", bI))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, client.ParsedPublicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not configure expire messages on queue. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = client.UDPConn.Write(ciphertext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not configure expire messages on queue. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = client.UDPConn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return errors.New(fmt.Sprintf("could not configure expire messages on queue. %s", err.Error()))
	}

	// Read from server
	res, err := bufio.NewReader(client.UDPConn).ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			attempts += 1

			if attempts < 10 {
				goto try
			} else {
				return errors.New(fmt.Sprintf("could not configure expire messages on queue. %s", err.Error()))

			}
		} else {
			return errors.New(fmt.Sprintf("could not configure expire messages on queue. %s", err.Error()))
		}
	}

	if bytes.HasPrefix(res, []byte("ACK")) {
		return nil
	} else {
		return errors.New("invalid response.  expecting ack.")
	}

	return nil
}

// SetExpireMessagesSeconds sets queue expire messages seconds configuration
func (client *Client) SetExpireMessagesSeconds(sec uint) error {

	attempts := 0 // Max attempts to reach server is 10

	plaintext := []byte(fmt.Sprintf("EXP MSGS SEC %d\r\n", sec))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, client.ParsedPublicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not configure expiry in seconds for messages on queue. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = client.UDPConn.Write(ciphertext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not configure expiry in seconds for messages on queue. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = client.UDPConn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return errors.New(fmt.Sprintf("could not configure expiry in seconds for messages on queue. %s", err.Error()))
	}

	// Read from server
	res, err := bufio.NewReader(client.UDPConn).ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			attempts += 1

			if attempts < 10 {
				goto try
			} else {
				return errors.New(fmt.Sprintf("could not configure expiry in seconds for messages on queue. %s", err.Error()))

			}
		} else {
			return errors.New(fmt.Sprintf("could not configure expiry in seconds for messages on queue. %s", err.Error()))
		}
	}

	if bytes.HasPrefix(res, []byte("ACK")) {
		return nil
	} else {
		return errors.New("invalid response.  expecting ack.")
	}

	return nil
}

// Shift removes first up in queue
func (client *Client) Shift() error {

	attempts := 0 // Max attempts to reach server is 10

	plaintext := []byte(fmt.Sprintf("SHIFT\r\n"))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, client.ParsedPublicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not shift queue. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = client.UDPConn.Write(ciphertext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not shift queue. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = client.UDPConn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return errors.New(fmt.Sprintf("could not shift queue. %s", err.Error()))
	}

	// Read from server
	res, err := bufio.NewReader(client.UDPConn).ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			attempts += 1

			if attempts < 10 {
				goto try
			} else {
				return errors.New(fmt.Sprintf("could not shift queue. %s", err.Error()))

			}
		} else {
			return errors.New(fmt.Sprintf("could not shift queue. %s", err.Error()))
		}
	}

	if bytes.HasPrefix(res, []byte("ACK")) {
		return nil
	} else {
		return errors.New("invalid response.  expecting ack.")
	}

	return nil
}

// Closes up connection
func (client *Client) Close() {

	client.UDPConn.Close()

}

// Clear clears entire queue
func (client *Client) Clear() error {

	attempts := 0 // Max attempts to reach server is 10

	plaintext := []byte(fmt.Sprintf("CLEAR\r\n"))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, client.ParsedPublicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not clear queue. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = client.UDPConn.Write(ciphertext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not clear queue. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = client.UDPConn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return errors.New(fmt.Sprintf("could not clear queue. %s", err.Error()))
	}

	// Read from server
	res, err := bufio.NewReader(client.UDPConn).ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			attempts += 1

			if attempts < 10 {
				goto try
			} else {
				return errors.New(fmt.Sprintf("could not clear queue. %s", err.Error()))
			}
		} else {
			return errors.New(fmt.Sprintf("could not clear queue. %s", err.Error()))
		}
	}

	if bytes.HasPrefix(res, []byte("ACK")) {
		return nil
	} else {
		return errors.New("invalid response.  expecting ack.")
	}

	return nil
}

// Pop removes last message from queue
func (client *Client) Pop() error {
	attempts := 0 // Max attempts to reach server is 10

	plaintext := []byte(fmt.Sprintf("POP\r\n"))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, client.ParsedPublicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not pop queue. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = client.UDPConn.Write(ciphertext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not pop queue. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = client.UDPConn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return errors.New(fmt.Sprintf("could not pop queue. %s", err.Error()))
	}

	// Read from server
	res, err := bufio.NewReader(client.UDPConn).ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			attempts += 1

			if attempts < 10 {
				goto try
			} else {
				return errors.New(fmt.Sprintf("could not pop queue. %s", err.Error()))
			}
		} else {
			return errors.New(fmt.Sprintf("could not pop queue. %s", err.Error()))
		}
	}

	if bytes.HasPrefix(res, []byte("ACK")) {
		return nil
	} else {
		return errors.New("invalid response.  expecting ack.")
	}

	return nil
}

// LastIn get last in queue
func (client *Client) LastIn() ([]byte, error) {

	attempts := 0 // Max attempts to reach server is 10

	plaintext := []byte(fmt.Sprintf("LAST IN\r\n"))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, client.ParsedPublicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get last message in queue. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = client.UDPConn.Write(ciphertext)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get last message in queue. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = client.UDPConn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get last message in queue. %s", err.Error()))
	}

	// Read from server
	res, err := bufio.NewReader(client.UDPConn).ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			attempts += 1

			if attempts < 10 {
				goto try
			} else {
				return nil, errors.New(fmt.Sprintf("could not get last message in queue. %s", err.Error()))
			}
		} else {
			return nil, errors.New(fmt.Sprintf("could not get last message in queue. %s", err.Error()))
		}
	}

	return res, nil
}

// ListConsumers lists queue consumers
func (client *Client) ListConsumers() ([]byte, error) {

	attempts := 0 // Max attempts to reach server is 10

	plaintext := []byte(fmt.Sprintf("LIST CONSUMERS\r\n"))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, client.ParsedPublicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not list consumers. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = client.UDPConn.Write(ciphertext)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not list consumers. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = client.UDPConn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not list consumers. %s", err.Error()))
	}

	// Read from server
	res, err := bufio.NewReader(client.UDPConn).ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			attempts += 1

			if attempts < 10 {
				goto try
			} else {
				return nil, errors.New(fmt.Sprintf("could not list consumers. %s", err.Error()))
			}
		} else {
			return nil, errors.New(fmt.Sprintf("could not list consumers. %s", err.Error()))
		}
	}

	return res, nil
}

// NewConsumer adds a new consumer
func (client *Client) NewConsumer(address string) error {

	attempts := 0 // Max attempts to reach server is 10

	plaintext := []byte(fmt.Sprintf("NEW CONSUMER %s\r\n", address))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, client.ParsedPublicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not add new consumer. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = client.UDPConn.Write(ciphertext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not add new consumer. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = client.UDPConn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return errors.New(fmt.Sprintf("could not add new consumer. %s", err.Error()))
	}

	// Read from server
	res, err := bufio.NewReader(client.UDPConn).ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			attempts += 1

			if attempts < 10 {
				goto try
			} else {
				return errors.New(fmt.Sprintf("could not add new consumer. %s", err.Error()))
			}
		} else {
			return errors.New(fmt.Sprintf("could not add new consumer. %s", err.Error()))
		}
	}

	if bytes.HasPrefix(res, []byte("ACK")) {
		return nil
	} else {
		return errors.New("invalid response.  expecting ack.")
	}

	return nil
}

// RemoveConsumer removes consumer
func (client *Client) RemoveConsumer(address string) error {

	attempts := 0 // Max attempts to reach server is 10

	plaintext := []byte(fmt.Sprintf("REM CONSUMER %s\r\n", address))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, client.ParsedPublicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not remove consumer. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = client.UDPConn.Write(ciphertext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not remove consumer. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = client.UDPConn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return errors.New(fmt.Sprintf("could not remove consumer. %s", err.Error()))
	}

	// Read from server
	res, err := bufio.NewReader(client.UDPConn).ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			attempts += 1

			if attempts < 10 {
				goto try
			} else {
				return errors.New(fmt.Sprintf("could not remove consumer. %s", err.Error()))
			}
		} else {
			return errors.New(fmt.Sprintf("could not remove consumer. %s", err.Error()))
		}
	}

	if bytes.HasPrefix(res, []byte("ACK")) {
		return nil
	} else {
		return errors.New("invalid response.  expecting ack.")
	}

	return nil
}
