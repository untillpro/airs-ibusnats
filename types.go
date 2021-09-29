/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package ibusnats

import "strings"

type NATSServers []string
type QueuesPartitionsMap map[string]int
type CurrentQueueName string
type Verbose bool

// Set complies to flag.Value interface. Provides ability specify as comma-separated urls in cmd line
func (n *NATSServers) Set(str string) error {
	*n = strings.Split(str, ",")
	return nil
}

// String complies to flag.Value interface. Provides ability specify as comma-separated urls in cmd line
func (n *NATSServers) String() string {
	return strings.Join(*n, ",")
}
