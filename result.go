/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package celery

const (
	// Statuses
	PENDING  = "PENDING"  // Task state is unknown (assumed pending since you know the id).
	RECEIVED = "RECEIVED" // Task was received by a worker.
	STARTED  = "STARTED"  // Task was started by a worker (:setting:`CELERY_TRACK_STARTED`).
	SUCCESS  = "SUCCESS"  // Task succeeded
	FAILURE  = "FAILURE"  // Task failed
	REVOKED  = "REVOKED"  // Task was revoked.
	RETRY    = "RETRY"    // Task is waiting for retry.
	IGNORED  = "IGNORED"
	REJECTED = "REJECTED"
)

type ResultMeta struct {
	Status    string `json:"status"`
	Result    Result `json:"result"`
	Traceback string `json:"trackback"`
	TaskId    string `json:"task_id"`
	// Children  interface{} // Not implemented
}

// Result is the result type returned by tasks. The result encoder should be able to encode it.
type Result interface{}

// Backend is the interface for publishers of tasks results.
type Backend interface {
	Publish(Task, *ResultMeta)
}
