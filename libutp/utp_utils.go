// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

// This is a port of a file in the C++ libutp library as found in the Transmission app.
// Copyright (c) 2010 BitTorrent, Inc.

package libutp

import (
	"net"

	"github.com/valyala/fastrand"
)

const (
	ethernetMTU     = 1500
	ipv4HeaderSize  = 20
	ipv6HeaderSize  = 40
	udpHeaderSize   = 8
	greHeaderSize   = 24
	pppoeHeaderSize = 8
	mppeHeaderSize  = 2

	fudgeHeaderSize = 36
	teredoMTU       = 1280

	udpIPv4Overhead   = ipv4HeaderSize + udpHeaderSize
	udpIPv6Overhead   = ipv6HeaderSize + udpHeaderSize
	udpTeredoOverhead = udpIPv4Overhead + udpIPv6Overhead

	udpIPv4MTU   = ethernetMTU - ipv4HeaderSize - udpHeaderSize - greHeaderSize - pppoeHeaderSize - mppeHeaderSize - fudgeHeaderSize
	udpIPv6MTU   = ethernetMTU - ipv6HeaderSize - udpHeaderSize - greHeaderSize - pppoeHeaderSize - mppeHeaderSize - fudgeHeaderSize
	UdpTeredoMTU = teredoMTU - ipv6HeaderSize - udpHeaderSize
)

var randGenerator = fastrand.RNG{}

// GetUDPMTU returns a best guess as to the MTU (maximum transmission unit) on
// the network to which the specified address belongs (IPv4 or IPv6).
func GetUDPMTU(addr *net.UDPAddr) uint16 {
	// Since we don't know the local address of the interface,
	// be conservative and assume all IPv6 connections are Teredo.
	if isIPv6(addr.IP) {
		return UdpTeredoMTU
	}
	return udpIPv4MTU
}

func getUDPOverhead(addr *net.UDPAddr) uint16 {
	// Since we don't know the local address of the interface,
	// be conservative and assume all IPv6 connections are Teredo.
	if isIPv6(addr.IP) {
		return udpTeredoOverhead
	}
	return udpIPv4Overhead
}

func RandomUint32() uint32 {
	return randGenerator.Uint32()
}

func RandomUint16() uint16 {
	return uint16(randGenerator.Uint32n(65535))
}

func getMaxPacketSize() int { return 980 }

func delaySample(addr *net.UDPAddr, sampleMS int) {}

func isIPv6(ip net.IP) bool {
	return ip.To4() == nil
}
