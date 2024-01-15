package fairymqgo

type opCode string

const (
	enqueueOpCode         = opCode("ENQUEUE\r\n%d\r\n%s")
	enqueueWithKeyOpCode  = opCode("ENQUEUE %s\r\n%d\r\n%s")
	lenghOpCode           = opCode("LENGTH\r\n")
	firstInOpCode         = opCode("FIRST IN\r\n")
	messagesByKeyOpCode   = opCode("MSGS WITH KEY %s\r\n")
	expireMsgOpCode       = opCode("EXP MSGS %d\r\n")
	expiresSecsMsgOpCode  = opCode("EXP MSGS SEC %d\r\n")
	shiftOpCode           = opCode("SHIFT\r\n")
	clearOpCode           = opCode("CLEAR\r\n")
	popOpCode             = opCode("POP\r\n")
	lastInOpCode          = opCode("LAST IN\r\n")
	listConsumersOpCode   = opCode("LIST CONSUMERS\r\n")
	newConsumerOpCode     = opCode("NEW CONSUMER %s\r\n")
	removeConsumerpOpCode = opCode("REM CONSUMER %s\r\n")
	ackOpcode             = opCode("ACK")
)
