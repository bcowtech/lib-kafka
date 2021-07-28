package kafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

const (
	// ErrBadMsg Local: Bad message format
	ErrBadMsg ErrorCode = ErrorCode(kafka.ErrBadMsg)
	// ErrBadCompression Local: Invalid compressed data
	ErrBadCompression ErrorCode = ErrorCode(kafka.ErrBadCompression)
	// ErrDestroy Local: Broker handle destroyed
	ErrDestroy ErrorCode = ErrorCode(kafka.ErrDestroy)
	// ErrFail Local: Communication failure with broker
	ErrFail ErrorCode = ErrorCode(kafka.ErrFail)
	// ErrTransport Local: Broker transport failure
	ErrTransport ErrorCode = ErrorCode(kafka.ErrTransport)
	// ErrCritSysResource Local: Critical system resource failure
	ErrCritSysResource ErrorCode = ErrorCode(kafka.ErrCritSysResource)
	// ErrResolve Local: Host resolution failure
	ErrResolve ErrorCode = ErrorCode(kafka.ErrResolve)
	// ErrMsgTimedOut Local: Message timed out
	ErrMsgTimedOut ErrorCode = ErrorCode(kafka.ErrMsgTimedOut)
	// ErrPartitionEOF Broker: No more messages
	ErrPartitionEOF ErrorCode = ErrorCode(kafka.ErrPartitionEOF)
	// ErrUnknownPartition Local: Unknown partition
	ErrUnknownPartition ErrorCode = ErrorCode(kafka.ErrUnknownPartition)
	// ErrFs Local: File or filesystem error
	ErrFs ErrorCode = ErrorCode(kafka.ErrFs)
	// ErrUnknownTopic Local: Unknown topic
	ErrUnknownTopic ErrorCode = ErrorCode(kafka.ErrUnknownTopic)
	// ErrAllBrokersDown Local: All broker connections are down
	ErrAllBrokersDown ErrorCode = ErrorCode(kafka.ErrAllBrokersDown)
	// ErrInvalidArg Local: Invalid argument or configuration
	ErrInvalidArg ErrorCode = ErrorCode(kafka.ErrInvalidArg)
	// ErrTimedOut Local: Timed out
	ErrTimedOut ErrorCode = ErrorCode(kafka.ErrTimedOut)
	// ErrQueueFull Local: Queue full
	ErrQueueFull ErrorCode = ErrorCode(kafka.ErrQueueFull)
	// ErrIsrInsuff Local: ISR count insufficient
	ErrIsrInsuff ErrorCode = ErrorCode(kafka.ErrIsrInsuff)
	// ErrNodeUpdate Local: Broker node update
	ErrNodeUpdate ErrorCode = ErrorCode(kafka.ErrNodeUpdate)
	// ErrSsl Local: SSL error
	ErrSsl ErrorCode = ErrorCode(kafka.ErrSsl)
	// ErrWaitCoord Local: Waiting for coordinator
	ErrWaitCoord ErrorCode = ErrorCode(kafka.ErrWaitCoord)
	// ErrUnknownGroup Local: Unknown group
	ErrUnknownGroup ErrorCode = ErrorCode(kafka.ErrUnknownGroup)
	// ErrInProgress Local: Operation in progress
	ErrInProgress ErrorCode = ErrorCode(kafka.ErrInProgress)
	// ErrPrevInProgress Local: Previous operation in progress
	ErrPrevInProgress ErrorCode = ErrorCode(kafka.ErrPrevInProgress)
	// ErrExistingSubscription Local: Existing subscription
	ErrExistingSubscription ErrorCode = ErrorCode(kafka.ErrExistingSubscription)
	// ErrAssignPartitions Local: Assign partitions
	ErrAssignPartitions ErrorCode = ErrorCode(kafka.ErrAssignPartitions)
	// ErrRevokePartitions Local: Revoke partitions
	ErrRevokePartitions ErrorCode = ErrorCode(kafka.ErrRevokePartitions)
	// ErrConflict Local: Conflicting use
	ErrConflict ErrorCode = ErrorCode(kafka.ErrConflict)
	// ErrState Local: Erroneous state
	ErrState ErrorCode = ErrorCode(kafka.ErrState)
	// ErrUnknownProtocol Local: Unknown protocol
	ErrUnknownProtocol ErrorCode = ErrorCode(kafka.ErrUnknownProtocol)
	// ErrNotImplemented Local: Not implemented
	ErrNotImplemented ErrorCode = ErrorCode(kafka.ErrNotImplemented)
	// ErrAuthentication Local: Authentication failure
	ErrAuthentication ErrorCode = ErrorCode(kafka.ErrAuthentication)
	// ErrNoOffset Local: No offset stored
	ErrNoOffset ErrorCode = ErrorCode(kafka.ErrNoOffset)
	// ErrOutdated Local: Outdated
	ErrOutdated ErrorCode = ErrorCode(kafka.ErrOutdated)
	// ErrTimedOutQueue Local: Timed out in queue
	ErrTimedOutQueue ErrorCode = ErrorCode(kafka.ErrTimedOutQueue)
	// ErrUnsupportedFeature Local: Required feature not supported by broker
	ErrUnsupportedFeature ErrorCode = ErrorCode(kafka.ErrUnsupportedFeature)
	// ErrWaitCache Local: Awaiting cache update
	ErrWaitCache ErrorCode = ErrorCode(kafka.ErrWaitCache)
	// ErrIntr Local: Operation interrupted
	ErrIntr ErrorCode = ErrorCode(kafka.ErrIntr)
	// ErrKeySerialization Local: Key serialization error
	ErrKeySerialization ErrorCode = ErrorCode(kafka.ErrKeySerialization)
	// ErrValueSerialization Local: Value serialization error
	ErrValueSerialization ErrorCode = ErrorCode(kafka.ErrValueSerialization)
	// ErrKeyDeserialization Local: Key deserialization error
	ErrKeyDeserialization ErrorCode = ErrorCode(kafka.ErrKeyDeserialization)
	// ErrValueDeserialization Local: Value deserialization error
	ErrValueDeserialization ErrorCode = ErrorCode(kafka.ErrValueDeserialization)
	// ErrPartial Local: Partial response
	ErrPartial ErrorCode = ErrorCode(kafka.ErrPartial)
	// ErrReadOnly Local: Read-only object
	ErrReadOnly ErrorCode = ErrorCode(kafka.ErrReadOnly)
	// ErrNoent Local: No such entry
	ErrNoent ErrorCode = ErrorCode(kafka.ErrNoent)
	// ErrUnderflow Local: Read underflow
	ErrUnderflow ErrorCode = ErrorCode(kafka.ErrUnderflow)
	// ErrInvalidType Local: Invalid type
	ErrInvalidType ErrorCode = ErrorCode(kafka.ErrInvalidType)
	// ErrRetry Local: Retry operation
	ErrRetry ErrorCode = ErrorCode(kafka.ErrRetry)
	// ErrPurgeQueue Local: Purged in queue
	ErrPurgeQueue ErrorCode = ErrorCode(kafka.ErrPurgeQueue)
	// ErrPurgeInflight Local: Purged in flight
	ErrPurgeInflight ErrorCode = ErrorCode(kafka.ErrPurgeInflight)
	// ErrFatal Local: Fatal error
	ErrFatal ErrorCode = ErrorCode(kafka.ErrFatal)
	// ErrInconsistent Local: Inconsistent state
	ErrInconsistent ErrorCode = ErrorCode(kafka.ErrInconsistent)
	// ErrGaplessGuarantee Local: Gap-less ordering would not be guaranteed if proceeding
	ErrGaplessGuarantee ErrorCode = ErrorCode(kafka.ErrGaplessGuarantee)
	// ErrMaxPollExceeded Local: Maximum application poll interval (max.poll.interval.ms) exceeded
	ErrMaxPollExceeded ErrorCode = ErrorCode(kafka.ErrMaxPollExceeded)
	// ErrUnknownBroker Local: Unknown broker
	ErrUnknownBroker ErrorCode = ErrorCode(kafka.ErrUnknownBroker)
	// ErrNotConfigured Local: Functionality not configured
	ErrNotConfigured ErrorCode = ErrorCode(kafka.ErrNotConfigured)
	// ErrFenced Local: This instance has been fenced by a newer instance
	ErrFenced ErrorCode = ErrorCode(kafka.ErrFenced)
	// ErrApplication Local: Application generated error
	ErrApplication ErrorCode = ErrorCode(kafka.ErrApplication)
	// ErrUnknown Unknown broker error
	ErrUnknown ErrorCode = ErrorCode(kafka.ErrUnknown)
	// ErrNoError Success
	ErrNoError ErrorCode = ErrorCode(kafka.ErrNoError)
	// ErrOffsetOutOfRange Broker: Offset out of range
	ErrOffsetOutOfRange ErrorCode = ErrorCode(kafka.ErrOffsetOutOfRange)
	// ErrInvalidMsg Broker: Invalid message
	ErrInvalidMsg ErrorCode = ErrorCode(kafka.ErrInvalidMsg)
	// ErrUnknownTopicOrPart Broker: Unknown topic or partition
	ErrUnknownTopicOrPart ErrorCode = ErrorCode(kafka.ErrUnknownTopicOrPart)
	// ErrInvalidMsgSize Broker: Invalid message size
	ErrInvalidMsgSize ErrorCode = ErrorCode(kafka.ErrInvalidMsgSize)
	// ErrLeaderNotAvailable Broker: Leader not available
	ErrLeaderNotAvailable ErrorCode = ErrorCode(kafka.ErrLeaderNotAvailable)
	// ErrNotLeaderForPartition Broker: Not leader for partition
	ErrNotLeaderForPartition ErrorCode = ErrorCode(kafka.ErrNotLeaderForPartition)
	// ErrRequestTimedOut Broker: Request timed out
	ErrRequestTimedOut ErrorCode = ErrorCode(kafka.ErrRequestTimedOut)
	// ErrBrokerNotAvailable Broker: Broker not available
	ErrBrokerNotAvailable ErrorCode = ErrorCode(kafka.ErrBrokerNotAvailable)
	// ErrReplicaNotAvailable Broker: Replica not available
	ErrReplicaNotAvailable ErrorCode = ErrorCode(kafka.ErrReplicaNotAvailable)
	// ErrMsgSizeTooLarge Broker: Message size too large
	ErrMsgSizeTooLarge ErrorCode = ErrorCode(kafka.ErrMsgSizeTooLarge)
	// ErrStaleCtrlEpoch Broker: StaleControllerEpochCode
	ErrStaleCtrlEpoch ErrorCode = ErrorCode(kafka.ErrStaleCtrlEpoch)
	// ErrOffsetMetadataTooLarge Broker: Offset metadata string too large
	ErrOffsetMetadataTooLarge ErrorCode = ErrorCode(kafka.ErrOffsetMetadataTooLarge)
	// ErrNetworkException Broker: Broker disconnected before response received
	ErrNetworkException ErrorCode = ErrorCode(kafka.ErrNetworkException)
	// ErrCoordinatorLoadInProgress Broker: Coordinator load in progress
	ErrCoordinatorLoadInProgress ErrorCode = ErrorCode(kafka.ErrCoordinatorLoadInProgress)
	// ErrCoordinatorNotAvailable Broker: Coordinator not available
	ErrCoordinatorNotAvailable ErrorCode = ErrorCode(kafka.ErrCoordinatorNotAvailable)
	// ErrNotCoordinator Broker: Not coordinator
	ErrNotCoordinator ErrorCode = ErrorCode(kafka.ErrNotCoordinator)
	// ErrTopicException Broker: Invalid topic
	ErrTopicException ErrorCode = ErrorCode(kafka.ErrTopicException)
	// ErrRecordListTooLarge Broker: Message batch larger than configured server segment size
	ErrRecordListTooLarge ErrorCode = ErrorCode(kafka.ErrRecordListTooLarge)
	// ErrNotEnoughReplicas Broker: Not enough in-sync replicas
	ErrNotEnoughReplicas ErrorCode = ErrorCode(kafka.ErrNotEnoughReplicas)
	// ErrNotEnoughReplicasAfterAppend Broker: Message(s) written to insufficient number of in-sync replicas
	ErrNotEnoughReplicasAfterAppend ErrorCode = ErrorCode(kafka.ErrNotEnoughReplicasAfterAppend)
	// ErrInvalidRequiredAcks Broker: Invalid required acks value
	ErrInvalidRequiredAcks ErrorCode = ErrorCode(kafka.ErrInvalidRequiredAcks)
	// ErrIllegalGeneration Broker: Specified group generation id is not valid
	ErrIllegalGeneration ErrorCode = ErrorCode(kafka.ErrIllegalGeneration)
	// ErrInconsistentGroupProtocol Broker: Inconsistent group protocol
	ErrInconsistentGroupProtocol ErrorCode = ErrorCode(kafka.ErrInconsistentGroupProtocol)
	// ErrInvalidGroupID Broker: Invalid group.id
	ErrInvalidGroupID ErrorCode = ErrorCode(kafka.ErrInvalidGroupID)
	// ErrUnknownMemberID Broker: Unknown member
	ErrUnknownMemberID ErrorCode = ErrorCode(kafka.ErrUnknownMemberID)
	// ErrInvalidSessionTimeout Broker: Invalid session timeout
	ErrInvalidSessionTimeout ErrorCode = ErrorCode(kafka.ErrInvalidSessionTimeout)
	// ErrRebalanceInProgress Broker: Group rebalance in progress
	ErrRebalanceInProgress ErrorCode = ErrorCode(kafka.ErrRebalanceInProgress)
	// ErrInvalidCommitOffsetSize Broker: Commit offset data size is not valid
	ErrInvalidCommitOffsetSize ErrorCode = ErrorCode(kafka.ErrInvalidCommitOffsetSize)
	// ErrTopicAuthorizationFailed Broker: Topic authorization failed
	ErrTopicAuthorizationFailed ErrorCode = ErrorCode(kafka.ErrTopicAuthorizationFailed)
	// ErrGroupAuthorizationFailed Broker: Group authorization failed
	ErrGroupAuthorizationFailed ErrorCode = ErrorCode(kafka.ErrGroupAuthorizationFailed)
	// ErrClusterAuthorizationFailed Broker: Cluster authorization failed
	ErrClusterAuthorizationFailed ErrorCode = ErrorCode(kafka.ErrClusterAuthorizationFailed)
	// ErrInvalidTimestamp Broker: Invalid timestamp
	ErrInvalidTimestamp ErrorCode = ErrorCode(kafka.ErrInvalidTimestamp)
	// ErrUnsupportedSaslMechanism Broker: Unsupported SASL mechanism
	ErrUnsupportedSaslMechanism ErrorCode = ErrorCode(kafka.ErrUnsupportedSaslMechanism)
	// ErrIllegalSaslState Broker: Request not valid in current SASL state
	ErrIllegalSaslState ErrorCode = ErrorCode(kafka.ErrIllegalSaslState)
	// ErrUnsupportedVersion Broker: API version not supported
	ErrUnsupportedVersion ErrorCode = ErrorCode(kafka.ErrUnsupportedVersion)
	// ErrTopicAlreadyExists Broker: Topic already exists
	ErrTopicAlreadyExists ErrorCode = ErrorCode(kafka.ErrTopicAlreadyExists)
	// ErrInvalidPartitions Broker: Invalid number of partitions
	ErrInvalidPartitions ErrorCode = ErrorCode(kafka.ErrInvalidPartitions)
	// ErrInvalidReplicationFactor Broker: Invalid replication factor
	ErrInvalidReplicationFactor ErrorCode = ErrorCode(kafka.ErrInvalidReplicationFactor)
	// ErrInvalidReplicaAssignment Broker: Invalid replica assignment
	ErrInvalidReplicaAssignment ErrorCode = ErrorCode(kafka.ErrInvalidReplicaAssignment)
	// ErrInvalidConfig Broker: Configuration is invalid
	ErrInvalidConfig ErrorCode = ErrorCode(kafka.ErrInvalidConfig)
	// ErrNotController Broker: Not controller for cluster
	ErrNotController ErrorCode = ErrorCode(kafka.ErrNotController)
	// ErrInvalidRequest Broker: Invalid request
	ErrInvalidRequest ErrorCode = ErrorCode(kafka.ErrInvalidRequest)
	// ErrUnsupportedForMessageFormat Broker: Message format on broker does not support request
	ErrUnsupportedForMessageFormat ErrorCode = ErrorCode(kafka.ErrUnsupportedForMessageFormat)
	// ErrPolicyViolation Broker: Policy violation
	ErrPolicyViolation ErrorCode = ErrorCode(kafka.ErrPolicyViolation)
	// ErrOutOfOrderSequenceNumber Broker: Broker received an out of order sequence number
	ErrOutOfOrderSequenceNumber ErrorCode = ErrorCode(kafka.ErrOutOfOrderSequenceNumber)
	// ErrDuplicateSequenceNumber Broker: Broker received a duplicate sequence number
	ErrDuplicateSequenceNumber ErrorCode = ErrorCode(kafka.ErrDuplicateSequenceNumber)
	// ErrInvalidProducerEpoch Broker: Producer attempted an operation with an old epoch
	ErrInvalidProducerEpoch ErrorCode = ErrorCode(kafka.ErrInvalidProducerEpoch)
	// ErrInvalidTxnState Broker: Producer attempted a transactional operation in an invalid state
	ErrInvalidTxnState ErrorCode = ErrorCode(kafka.ErrInvalidTxnState)
	// ErrInvalidProducerIDMapping Broker: Producer attempted to use a producer id which is not currently assigned to its transactional id
	ErrInvalidProducerIDMapping ErrorCode = ErrorCode(kafka.ErrInvalidProducerIDMapping)
	// ErrInvalidTransactionTimeout Broker: Transaction timeout is larger than the maximum value allowed by the broker's max.transaction.timeout.ms
	ErrInvalidTransactionTimeout ErrorCode = ErrorCode(kafka.ErrInvalidTransactionTimeout)
	// ErrConcurrentTransactions Broker: Producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing
	ErrConcurrentTransactions ErrorCode = ErrorCode(kafka.ErrConcurrentTransactions)
	// ErrTransactionCoordinatorFenced Broker: Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer
	ErrTransactionCoordinatorFenced ErrorCode = ErrorCode(kafka.ErrTransactionCoordinatorFenced)
	// ErrTransactionalIDAuthorizationFailed Broker: Transactional Id authorization failed
	ErrTransactionalIDAuthorizationFailed ErrorCode = ErrorCode(kafka.ErrTransactionalIDAuthorizationFailed)
	// ErrSecurityDisabled Broker: Security features are disabled
	ErrSecurityDisabled ErrorCode = ErrorCode(kafka.ErrSecurityDisabled)
	// ErrOperationNotAttempted Broker: Operation not attempted
	ErrOperationNotAttempted ErrorCode = ErrorCode(kafka.ErrOperationNotAttempted)
	// ErrKafkaStorageError Broker: Disk error when trying to access log file on disk
	ErrKafkaStorageError ErrorCode = ErrorCode(kafka.ErrKafkaStorageError)
	// ErrLogDirNotFound Broker: The user-specified log directory is not found in the broker config
	ErrLogDirNotFound ErrorCode = ErrorCode(kafka.ErrLogDirNotFound)
	// ErrSaslAuthenticationFailed Broker: SASL Authentication failed
	ErrSaslAuthenticationFailed ErrorCode = ErrorCode(kafka.ErrSaslAuthenticationFailed)
	// ErrUnknownProducerID Broker: Unknown Producer Id
	ErrUnknownProducerID ErrorCode = ErrorCode(kafka.ErrUnknownProducerID)
	// ErrReassignmentInProgress Broker: Partition reassignment is in progress
	ErrReassignmentInProgress ErrorCode = ErrorCode(kafka.ErrReassignmentInProgress)
	// ErrDelegationTokenAuthDisabled Broker: Delegation Token feature is not enabled
	ErrDelegationTokenAuthDisabled ErrorCode = ErrorCode(kafka.ErrDelegationTokenAuthDisabled)
	// ErrDelegationTokenNotFound Broker: Delegation Token is not found on server
	ErrDelegationTokenNotFound ErrorCode = ErrorCode(kafka.ErrDelegationTokenNotFound)
	// ErrDelegationTokenOwnerMismatch Broker: Specified Principal is not valid Owner/Renewer
	ErrDelegationTokenOwnerMismatch ErrorCode = ErrorCode(kafka.ErrDelegationTokenOwnerMismatch)
	// ErrDelegationTokenRequestNotAllowed Broker: Delegation Token requests are not allowed on this connection
	ErrDelegationTokenRequestNotAllowed ErrorCode = ErrorCode(kafka.ErrDelegationTokenRequestNotAllowed)
	// ErrDelegationTokenAuthorizationFailed Broker: Delegation Token authorization failed
	ErrDelegationTokenAuthorizationFailed ErrorCode = ErrorCode(kafka.ErrDelegationTokenAuthorizationFailed)
	// ErrDelegationTokenExpired Broker: Delegation Token is expired
	ErrDelegationTokenExpired ErrorCode = ErrorCode(kafka.ErrDelegationTokenExpired)
	// ErrInvalidPrincipalType Broker: Supplied principalType is not supported
	ErrInvalidPrincipalType ErrorCode = ErrorCode(kafka.ErrInvalidPrincipalType)
	// ErrNonEmptyGroup Broker: The group is not empty
	ErrNonEmptyGroup ErrorCode = ErrorCode(kafka.ErrNonEmptyGroup)
	// ErrGroupIDNotFound Broker: The group id does not exist
	ErrGroupIDNotFound ErrorCode = ErrorCode(kafka.ErrGroupIDNotFound)
	// ErrFetchSessionIDNotFound Broker: The fetch session ID was not found
	ErrFetchSessionIDNotFound ErrorCode = ErrorCode(kafka.ErrFetchSessionIDNotFound)
	// ErrInvalidFetchSessionEpoch Broker: The fetch session epoch is invalid
	ErrInvalidFetchSessionEpoch ErrorCode = ErrorCode(kafka.ErrInvalidFetchSessionEpoch)
	// ErrListenerNotFound Broker: No matching listener
	ErrListenerNotFound ErrorCode = ErrorCode(kafka.ErrListenerNotFound)
	// ErrTopicDeletionDisabled Broker: Topic deletion is disabled
	ErrTopicDeletionDisabled ErrorCode = ErrorCode(kafka.ErrTopicDeletionDisabled)
	// ErrFencedLeaderEpoch Broker: Leader epoch is older than broker epoch
	ErrFencedLeaderEpoch ErrorCode = ErrorCode(kafka.ErrFencedLeaderEpoch)
	// ErrUnknownLeaderEpoch Broker: Leader epoch is newer than broker epoch
	ErrUnknownLeaderEpoch ErrorCode = ErrorCode(kafka.ErrUnknownLeaderEpoch)
	// ErrUnsupportedCompressionType Broker: Unsupported compression type
	ErrUnsupportedCompressionType ErrorCode = ErrorCode(kafka.ErrUnsupportedCompressionType)
	// ErrStaleBrokerEpoch Broker: Broker epoch has changed
	ErrStaleBrokerEpoch ErrorCode = ErrorCode(kafka.ErrStaleBrokerEpoch)
	// ErrOffsetNotAvailable Broker: Leader high watermark is not caught up
	ErrOffsetNotAvailable ErrorCode = ErrorCode(kafka.ErrOffsetNotAvailable)
	// ErrMemberIDRequired Broker: Group member needs a valid member ID
	ErrMemberIDRequired ErrorCode = ErrorCode(kafka.ErrMemberIDRequired)
	// ErrPreferredLeaderNotAvailable Broker: Preferred leader was not available
	ErrPreferredLeaderNotAvailable ErrorCode = ErrorCode(kafka.ErrPreferredLeaderNotAvailable)
	// ErrGroupMaxSizeReached Broker: Consumer group has reached maximum size
	ErrGroupMaxSizeReached ErrorCode = ErrorCode(kafka.ErrGroupMaxSizeReached)
	// ErrFencedInstanceID Broker: Static consumer fenced by other consumer with same group.instance.id
	ErrFencedInstanceID ErrorCode = ErrorCode(kafka.ErrFencedInstanceID)
)
