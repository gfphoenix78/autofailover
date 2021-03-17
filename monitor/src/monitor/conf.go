package monitor

import "time"

const _DEFAULT_PRIMARY_TIMEOUT_FOR_PROMOTION = 10 * time.Second
const COLLECTOR_IDLE_TIMEOUT = 60 * time.Second
const COLLECTOR_RETRY_TIMEOUT = 3 * time.Second
