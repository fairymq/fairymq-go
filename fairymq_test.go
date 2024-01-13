// Package fairymqgo
// // // // // // // // // // // // // // // // // // // // // // // // // //
// fairyMQ GO Native Client Module Unit Tests
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
	"reflect"
	"testing"
)

func TestClient_Clear(t *testing.T) {
	type fields struct {
		Host      string
		PublicKey string
	}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
		wantErr bool
	}{
		{name: "test", wantErr: false, want: []byte("ACK"), fields: fields{Host: "0.0.0.0:5991",PublicKey: "testing.public.pem"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				Host:      tt.fields.Host,
				PublicKey: tt.fields.PublicKey,
			}
			got, err := client.Clear()
			if (err != nil) != tt.wantErr {
				t.Errorf("Clear() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Clear() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_Enqueue(t *testing.T) {
	type fields struct {
		Host      string
		PublicKey string
	}
	type args struct {
		data []byte
	}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
		wantErr bool
	}{
		{name: "test", wantErr: false, want: []byte("ACK"), fields: fields{Host: "0.0.0.0:5991",PublicKey: "testing.public.pem"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				Host:      tt.fields.Host,
				PublicKey: tt.fields.PublicKey,
			}
			if err := client.Enqueue(tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("Enqueue() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_FirstIn(t *testing.T) {
	type fields struct {
		Host      string
		PublicKey string
	}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
		wantErr bool
	}{
		{name: "test", wantErr: false, fields: fields{Host: "0.0.0.0:5991",PublicKey: "testing.public.pem"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				Host:      tt.fields.Host,
				PublicKey: tt.fields.PublicKey,
			}
			got, err := client.FirstIn()
			if (err != nil) != tt.wantErr {
				t.Errorf("FirstIn() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FirstIn() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_LastIn(t *testing.T) {
	type fields struct {
		Host      string
		PublicKey string
	}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
		wantErr bool
	}{
		{name: "test", wantErr: false, fields: fields{Host: "0.0.0.0:5991",PublicKey: "testing.public.pem"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				Host:      tt.fields.Host,
				PublicKey: tt.fields.PublicKey,
			}
			got, err := client.LastIn()
			if (err != nil) != tt.wantErr {
				t.Errorf("LastIn() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LastIn() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_Length(t *testing.T) {
	type fields struct {
		Host      string
		PublicKey string
	}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
		wantErr bool
	}{
		{name: "test", wantErr: false, want: []byte("0"), fields: fields{Host: "0.0.0.0:5991",PublicKey: "testing.public.pem"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				Host:      tt.fields.Host,
				PublicKey: tt.fields.PublicKey,
			}
			got, err := client.Length()
			if (err != nil) != tt.wantErr {
				t.Errorf("Length() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Length() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_Pop(t *testing.T) {
	type fields struct {
		Host      string
		PublicKey string
	}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
		wantErr bool
	}{
		{name: "test", wantErr: false, want: []byte("ACK"), fields: fields{Host: "0.0.0.0:5991",PublicKey: "testing.public.pem"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				Host:      tt.fields.Host,
				PublicKey: tt.fields.PublicKey,
			}
			got, err := client.Pop()
			if (err != nil) != tt.wantErr {
				t.Errorf("Pop() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Pop() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_Shift(t *testing.T) {
	type fields struct {
		Host      string
		PublicKey string
	}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
		wantErr bool
	}{
		{name: "test", wantErr: false, want: []byte("ACK"), fields: fields{Host: "0.0.0.0:5991",PublicKey: "testing.public.pem"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				Host:      tt.fields.Host,
				PublicKey: tt.fields.PublicKey,
			}
			got, err := client.Shift()
			if (err != nil) != tt.wantErr {
				t.Errorf("Shift() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Shift() got = %v, want %v", got, tt.want)
			}
		})
	}
}
