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
	"fmt"
	"net"
	"os"
	"time"
)

type Client struct {
	udpConn *net.UDPConn
	pubKey  *rsa.PublicKey

	options options
}

func Dial(host, publicKey string, opts ...Option) (*Client, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", host)
	if err != nil {
		return nil, fmt.Errorf("fairymq: resolve udp addr error: %w", err)
	}

	udpConn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, fmt.Errorf("fairymq: dial udp error: %w", err)
	}

	f, err := os.ReadFile(publicKey)
	if err != nil {
		return nil, fmt.Errorf("fairymq: read public key file %s error: %w", publicKey, err)
	}

	pemBlock, _ := pem.Decode(f)
	parsedPublicKey, err := x509.ParsePKIXPublicKey(pemBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("fairymq: parse public key error: %w", err)
	}

	rsaPublicKey, ok := parsedPublicKey.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("fairymq: parsed public key is not an rsa key: %T", parsedPublicKey)
	}

	o := options{
		attempts:    10,
		readTimeout: 60 * time.Millisecond,
	}

	for i := range opts {
		opts[i](&o)
	}

	return &Client{
		udpConn: udpConn,
		pubKey:  rsaPublicKey,
		options: o,
	}, nil
}

// Enqueue enqueues a new message into queue
func (client *Client) Enqueue(data string) error {
	command := fmt.Sprintf(string(enqueueOpCode), time.Now().UnixMicro(), data)

	p, err := client.writeWithRetry(command)
	if err != nil {
		return fmt.Errorf("fairymq: enqueue data error: %w", err)
	}

	if !bytes.HasPrefix(p, []byte(ackOpcode)) {
		return fmt.Errorf("fairymq: invalid response for command %s received: %v", command, p)
	}

	return nil
}

// EnqueueWithKey enqueues a new message into queue with a provided key.  Keys are not unique
func (client *Client) EnqueueWithKey(data, messageKey string) error {
	command := fmt.Sprintf(string(enqueueWithKeyOpCode), messageKey, time.Now().UnixMicro(), data)

	p, err := client.writeWithRetry(command)
	if err != nil {
		return fmt.Errorf("fairymq: enqueue data with key error: %w", err)
	}

	if !bytes.HasPrefix(p, []byte(ackOpcode)) {
		return fmt.Errorf("fairymq: invalid response for command %s received: %v", command, p)
	}

	return nil
}

// Length get length of queue
func (client *Client) Length() ([]byte, error) {
	command := string(lenghOpCode)

	p, err := client.writeWithRetry(command)
	if err != nil {
		return nil, fmt.Errorf("fairymq: length error: %w", err)
	}

	// TOOD: parse and return uint.
	return p, nil
}

// FirstIn first message up in queue
func (client *Client) FirstIn() ([]byte, error) {
	command := string(firstInOpCode)

	p, err := client.writeWithRetry(command)
	if err != nil {
		return nil, fmt.Errorf("fairymq: first in error: %w", err)
	}

	// TOOD: parse and return.
	return p, nil
}

func (client *Client) GetAllMessagesByKey(key string) ([][]byte, error) {
	command := fmt.Sprintf(string(messagesByKeyOpCode), key)

	p, err := client.writeWithRetry(command)
	if err != nil {
		return nil, fmt.Errorf("fairymq: messages by key error: %w", err)
	}

	// TOOD: parse and return normalized messages.
	return bytes.Split(bytes.TrimSuffix(p, []byte("\r\n")), []byte("\r\r")), nil
}

// ExpireMessages sets whether queue expires messages or not.  Default is 7200 seconds
func (client *Client) ExpireMessages(enabled bool) error {
	f := 0
	if enabled {
		f = 1
	}

	command := fmt.Sprintf(string(expireMsgOpCode), f)

	p, err := client.writeWithRetry(command)
	if err != nil {
		return fmt.Errorf("fairymq: expire messages error: %w", err)
	}

	if !bytes.HasPrefix(p, []byte(ackOpcode)) {
		return fmt.Errorf("fairymq: invalid response for command %s received: %v", command, p)
	}

	return nil
}

// SetExpireMessagesSeconds sets queue expire messages seconds configuration
func (client *Client) SetExpireMessagesSeconds(sec uint) error {
	command := fmt.Sprintf(string(expiresSecsMsgOpCode), sec)

	p, err := client.writeWithRetry(command)
	if err != nil {
		return fmt.Errorf("fairymq: expire secs messages error: %w", err)
	}

	if !bytes.HasPrefix(p, []byte(ackOpcode)) {
		return fmt.Errorf("fairymq: invalid response for command %s received: %v", command, p)
	}

	return nil
}

// Shift removes first up in queue
func (client *Client) Shift() error {
	command := fmt.Sprintf(string(shiftOpCode))

	p, err := client.writeWithRetry(command)
	if err != nil {
		return fmt.Errorf("fairymq: shift error: %w", err)
	}

	if !bytes.HasPrefix(p, []byte(ackOpcode)) {
		return fmt.Errorf("fairymq: invalid response for command %s received: %v", command, p)
	}

	return nil
}

// Clear clears entire queue
func (client *Client) Clear() error {
	command := fmt.Sprintf(string(clearOpCode))

	p, err := client.writeWithRetry(command)
	if err != nil {
		return fmt.Errorf("fairymq: clear error: %w", err)
	}

	if !bytes.HasPrefix(p, []byte(ackOpcode)) {
		return fmt.Errorf("fairymq: invalid response for command %s received: %v", command, p)
	}

	return nil
}

// Pop removes last message from queue
func (client *Client) Pop() error {
	command := fmt.Sprintf(string(popOpCode))

	p, err := client.writeWithRetry(command)
	if err != nil {
		return fmt.Errorf("fairymq: pop queue error: %w", err)
	}

	if !bytes.HasPrefix(p, []byte(ackOpcode)) {
		return fmt.Errorf("fairymq: invalid response for command %s received: %v", command, p)
	}

	return nil
}

// LastIn get last in queue
func (client *Client) LastIn() ([]byte, error) {
	command := fmt.Sprintf(string(lastInOpCode))

	p, err := client.writeWithRetry(command)
	if err != nil {
		return nil, fmt.Errorf("fairymq: last in error: %w", err)
	}

	return p, nil
}

// ListConsumers lists queue consumers
func (client *Client) ListConsumers() ([]byte, error) {
	command := fmt.Sprintf(string(listConsumersOpCode))

	p, err := client.writeWithRetry(command)
	if err != nil {
		return nil, fmt.Errorf("fairymq: list consumers error: %w", err)
	}

	return p, nil
}

// NewConsumer adds a new consumer
func (client *Client) NewConsumer(address string) error {
	command := fmt.Sprintf(string(newConsumerOpCode), address)

	p, err := client.writeWithRetry(command)
	if err != nil {
		return fmt.Errorf("fairymq: new consumer error: %w", err)
	}

	if !bytes.HasPrefix(p, []byte(ackOpcode)) {
		return fmt.Errorf("fairymq: invalid response for command %s received: %v", command, p)
	}

	return nil
}

// RemoveConsumer removes consumer
func (client *Client) RemoveConsumer(address string) error {
	command := fmt.Sprintf(string(removeConsumerpOpCode), address)

	p, err := client.writeWithRetry(command)
	if err != nil {
		return fmt.Errorf("fairymq: remove consumer error: %w", err)
	}

	if !bytes.HasPrefix(p, []byte(ackOpcode)) {
		return fmt.Errorf("fairymq: invalid response for command %s received: %v", command, p)
	}

	return nil
}

// Closes up connection
func (client *Client) Close() error {
	return client.udpConn.Close()
}

func (client *Client) writeWithRetry(command string) ([]byte, error) {
	encryptedCommand, err := rsa.EncryptPKCS1v15(rand.Reader, client.pubKey, []byte(command))
	if err != nil {
		return nil, fmt.Errorf("could not encrypt command %s: %w", command, err)
	}

	for i := 0; i < int(client.options.attempts); i++ {
		_, err = client.udpConn.Write(encryptedCommand)
		if err != nil {
			return nil, fmt.Errorf("could not write command %s: %w", command, err)
		}

		if err := client.udpConn.SetReadDeadline(time.Now().Add(client.options.readTimeout)); err != nil {
			return nil, fmt.Errorf("set read deadline error: %w", err)
		}

		p, err := bufio.NewReader(client.udpConn).ReadBytes('\n')
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}

			return nil, fmt.Errorf("could not read response: %w", err)
		}

		return p, nil
	}

	return nil, fmt.Errorf("exceeded the maximum number of attempts when attempting to send a command: %s", command)
}
