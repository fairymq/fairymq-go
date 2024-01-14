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
	Host string // i.e 0.0.0.0:5991
	PublicKey string //i.e example.public.pem
}

// Enqueue enqueues a new message into queue
func (client *Client) Enqueue(data []byte) error {

	attempts := 0 // Max attempts to reach server is 10

	// Resolve UDP address
	udpAddr, err := net.ResolveUDPAddr("udp", client.Host)
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	// Dial address
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	// Mark the creation of message
	timestamp := time.Now().UnixMicro()

	publicKeyPEM, err := os.ReadFile(client.PublicKey)
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	publicKeyBlock, _ := pem.Decode(publicKeyPEM)
	publicKey, err := x509.ParsePKIXPublicKey(publicKeyBlock.Bytes)
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	plaintext := append([]byte(fmt.Sprintf("ENQUEUE\r\n%d\r\n", timestamp)), data...)
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, publicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = conn.Write(ciphertext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = conn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	// Read from server
	_, err = bufio.NewReader(conn).ReadString('\n')
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

	return nil
}

// EnqueueWithKey enqueues a new message into queue with a provided key.  Keys are not unique
func (client *Client) EnqueueWithKey(data []byte, messageKey string) error {

	attempts := 0 // Max attempts to reach server is 10

	// Resolve UDP address
	udpAddr, err := net.ResolveUDPAddr("udp", client.Host)
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	// Dial address
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	// Mark the creation of message
	timestamp := time.Now().UnixMicro()

	publicKeyPEM, err := os.ReadFile(client.PublicKey)
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	publicKeyBlock, _ := pem.Decode(publicKeyPEM)
	publicKey, err := x509.ParsePKIXPublicKey(publicKeyBlock.Bytes)
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	plaintext := append([]byte(fmt.Sprintf("ENQUEUE %s\r\n%d\r\n",messageKey, timestamp)), data...)
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, publicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = conn.Write(ciphertext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = conn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return errors.New(fmt.Sprintf("could not enqueue message. %s", err.Error()))
	}

	// Read from server
	_, err = bufio.NewReader(conn).ReadString('\n')
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

	return nil
}

// Length get length of queue
func (client *Client) Length() ([]byte, error) {

	attempts := 0 // Max attempts to reach server is 10

	// Resolve UDP address
	udpAddr, err := net.ResolveUDPAddr("udp", client.Host)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get length of queue. %s", err.Error()))
	}

	// Dial address
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get length of queue. %s", err.Error()))
	}

	publicKeyPEM, err := os.ReadFile(client.PublicKey)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get length of queue. %s", err.Error()))
	}

	publicKeyBlock, _ := pem.Decode(publicKeyPEM)
	publicKey, err := x509.ParsePKIXPublicKey(publicKeyBlock.Bytes)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get length of queue. %s", err.Error()))
	}

	plaintext := []byte(fmt.Sprintf("LENGTH\r\n"))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, publicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get length of queue. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = conn.Write(ciphertext)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get length of queue. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = conn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get length of queue. %s", err.Error()))
	}

	// Read from server
	res, err := bufio.NewReader(conn).ReadBytes('\n')
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

	// Resolve UDP address
	udpAddr, err := net.ResolveUDPAddr("udp", client.Host)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get first message in queue. %s", err.Error()))
	}

	// Dial address
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get first message in queue. %s", err.Error()))
	}

	publicKeyPEM, err := os.ReadFile(client.PublicKey)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get first message in queue. %s", err.Error()))
	}

	publicKeyBlock, _ := pem.Decode(publicKeyPEM)
	publicKey, err := x509.ParsePKIXPublicKey(publicKeyBlock.Bytes)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get first message in queue. %s", err.Error()))
	}

	plaintext := []byte(fmt.Sprintf("FIRST IN\r\n"))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, publicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get first message in queue. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = conn.Write(ciphertext)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get first message in queue. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = conn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get first message in queue. %s", err.Error()))
	}

	// Read from server
	res, err := bufio.NewReader(conn).ReadBytes('\n')
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

// Shift removes first up in queue
func (client *Client) Shift()  error {

	attempts := 0 // Max attempts to reach server is 10

	// Resolve UDP address
	udpAddr, err := net.ResolveUDPAddr("udp", client.Host)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not shift queue. %s", err.Error()))
	}

	// Dial address
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not shift queue. %s", err.Error()))
	}

	publicKeyPEM, err := os.ReadFile(client.PublicKey)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not shift queue. %s", err.Error()))
	}

	publicKeyBlock, _ := pem.Decode(publicKeyPEM)
	publicKey, err := x509.ParsePKIXPublicKey(publicKeyBlock.Bytes)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not shift queue. %s", err.Error()))
	}

	plaintext := []byte(fmt.Sprintf("SHIFT\r\n"))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, publicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not shift queue. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = conn.Write(ciphertext)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not shift queue. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = conn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return  errors.New(fmt.Sprintf("could not shift queue. %s", err.Error()))
	}

	// Read from server
	_, err = bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			attempts += 1

			if attempts < 10 {
				goto try
			} else {
				return  errors.New(fmt.Sprintf("could not shift queue. %s", err.Error()))

			}
		} else {
			return  errors.New(fmt.Sprintf("could not shift queue. %s", err.Error()))
		}
	}

	return  nil
}

// Clear clears entire queue
func (client *Client) Clear()  error {

	attempts := 0 // Max attempts to reach server is 10

	// Resolve UDP address
	udpAddr, err := net.ResolveUDPAddr("udp", client.Host)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not clear queue. %s", err.Error()))
	}

	// Dial address
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not clear queue. %s", err.Error()))
	}

	publicKeyPEM, err := os.ReadFile(client.PublicKey)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not clear queue. %s", err.Error()))
	}

	publicKeyBlock, _ := pem.Decode(publicKeyPEM)
	publicKey, err := x509.ParsePKIXPublicKey(publicKeyBlock.Bytes)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not clear queue. %s", err.Error()))
	}

	plaintext := []byte(fmt.Sprintf("CLEAR\r\n"))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, publicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not clear queue. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = conn.Write(ciphertext)
	if err != nil {
		return errors.New(fmt.Sprintf("could not clear queue. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = conn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return  errors.New(fmt.Sprintf("could not clear queue. %s", err.Error()))
	}

	// Read from server
	_, err = bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			attempts += 1

			if attempts < 10 {
				goto try
			} else {
				return  errors.New(fmt.Sprintf("could not clear queue. %s", err.Error()))
			}
		} else {
			return  errors.New(fmt.Sprintf("could not clear queue. %s", err.Error()))
		}
	}

	return  nil
}

// Pop removes last message from queue
func (client *Client) Pop() error {
	attempts := 0 // Max attempts to reach server is 10

	// Resolve UDP address
	udpAddr, err := net.ResolveUDPAddr("udp", client.Host)
	if err != nil {
		return errors.New(fmt.Sprintf("could not pop queue. %s", err.Error()))
	}

	// Dial address
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not pop queue. %s", err.Error()))
	}

	publicKeyPEM, err := os.ReadFile(client.PublicKey)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not pop queue. %s", err.Error()))
	}

	publicKeyBlock, _ := pem.Decode(publicKeyPEM)
	publicKey, err := x509.ParsePKIXPublicKey(publicKeyBlock.Bytes)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not pop queue. %s", err.Error()))
	}

	plaintext := []byte(fmt.Sprintf("POP\r\n"))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, publicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not pop queue. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = conn.Write(ciphertext)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not pop queue. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = conn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return  errors.New(fmt.Sprintf("could not pop queue. %s", err.Error()))
	}

	// Read from server
	_, err = bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			attempts += 1

			if attempts < 10 {
				goto try
			} else {
				return errors.New(fmt.Sprintf("could not pop queue. %s", err.Error()))
			}
		} else {
			return  errors.New(fmt.Sprintf("could not pop queue. %s", err.Error()))
		}
	}

	return  nil
}

// LastIn get last in queue
func (client *Client) LastIn() ([]byte, error) {

	attempts := 0 // Max attempts to reach server is 10

	// Resolve UDP address
	udpAddr, err := net.ResolveUDPAddr("udp", client.Host)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get last message in queue. %s", err.Error()))
	}

	// Dial address
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get last message in queue. %s", err.Error()))
	}

	publicKeyPEM, err := os.ReadFile(client.PublicKey)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get last message in queue. %s", err.Error()))
	}

	publicKeyBlock, _ := pem.Decode(publicKeyPEM)
	publicKey, err := x509.ParsePKIXPublicKey(publicKeyBlock.Bytes)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get last message in queue. %s", err.Error()))
	}

	plaintext := []byte(fmt.Sprintf("LAST IN\r\n"))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, publicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get last message in queue. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = conn.Write(ciphertext)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get last message in queue. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = conn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not get last message in queue. %s", err.Error()))
	}

	// Read from server
	res, err := bufio.NewReader(conn).ReadBytes('\n')
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

	// Resolve UDP address
	udpAddr, err := net.ResolveUDPAddr("udp", client.Host)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not list consumers. %s", err.Error()))
	}

	// Dial address
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not list consumers. %s", err.Error()))
	}

	publicKeyPEM, err := os.ReadFile(client.PublicKey)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not list consumers. %s", err.Error()))
	}

	publicKeyBlock, _ := pem.Decode(publicKeyPEM)
	publicKey, err := x509.ParsePKIXPublicKey(publicKeyBlock.Bytes)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not list consumers. %s", err.Error()))
	}

	plaintext := []byte(fmt.Sprintf("LIST CONSUMERS\r\n"))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, publicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not list consumers. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = conn.Write(ciphertext)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not list consumers. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = conn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return nil, errors.New(fmt.Sprintf("could not list consumers. %s", err.Error()))
	}

	// Read from server
	res, err := bufio.NewReader(conn).ReadBytes('\n')
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

	// Resolve UDP address
	udpAddr, err := net.ResolveUDPAddr("udp", client.Host)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not add new consumer. %s", err.Error()))
	}

	// Dial address
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not add new consumer. %s", err.Error()))
	}

	publicKeyPEM, err := os.ReadFile(client.PublicKey)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not add new consumer. %s", err.Error()))
	}

	publicKeyBlock, _ := pem.Decode(publicKeyPEM)
	publicKey, err := x509.ParsePKIXPublicKey(publicKeyBlock.Bytes)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not add new consumer. %s", err.Error()))
	}

	plaintext := []byte(fmt.Sprintf("NEW CONSUMER %s\r\n", address))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, publicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not add new consumer. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = conn.Write(ciphertext)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not add new consumer. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = conn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return  errors.New(fmt.Sprintf("could not add new consumer. %s", err.Error()))
	}

	// Read from server
	_, err = bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			attempts += 1

			if attempts < 10 {
				goto try
			} else {
				return  errors.New(fmt.Sprintf("could not add new consumer. %s", err.Error()))
			}
		} else {
			return  errors.New(fmt.Sprintf("could not add new consumer. %s", err.Error()))
		}
	}

	return  nil
}


// RemoveConsumer removes consumer
func (client *Client) RemoveConsumer(address string)  error {

	attempts := 0 // Max attempts to reach server is 10

	// Resolve UDP address
	udpAddr, err := net.ResolveUDPAddr("udp", client.Host)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not remove consumer. %s", err.Error()))
	}

	// Dial address
	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return errors.New(fmt.Sprintf("could not remove consumer. %s", err.Error()))
	}

	publicKeyPEM, err := os.ReadFile(client.PublicKey)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not remove consumer. %s", err.Error()))
	}

	publicKeyBlock, _ := pem.Decode(publicKeyPEM)
	publicKey, err := x509.ParsePKIXPublicKey(publicKeyBlock.Bytes)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not remove consumer. %s", err.Error()))
	}

	plaintext := []byte(fmt.Sprintf("REM CONSUMER %s\r\n", address))
	ciphertext, err := rsa.EncryptPKCS1v15(rand.Reader, publicKey.(*rsa.PublicKey), plaintext)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not remove consumer. %s", err.Error()))
	}

	// Attempt server
	goto try

try:

	// Send to server
	_, err = conn.Write(ciphertext)
	if err != nil {
		return  errors.New(fmt.Sprintf("could not remove consumer. %s", err.Error()))
	}

	// If nothing received in 60 milliseconds.  Retry
	err = conn.SetReadDeadline(time.Now().Add(60 * time.Millisecond))
	if err != nil {
		return  errors.New(fmt.Sprintf("could not remove consumer. %s", err.Error()))
	}

	// Read from server
	_, err = bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			attempts += 1

			if attempts < 10 {
				goto try
			} else {
				return  errors.New(fmt.Sprintf("could not remove consumer. %s", err.Error()))
			}
		} else {
			return  errors.New(fmt.Sprintf("could not remove consumer. %s", err.Error()))
		}
	}

	return  nil
}
