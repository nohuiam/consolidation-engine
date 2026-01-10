/**
 * InterLock Module
 *
 * UDP mesh communication for BOP server coordination.
 */

export { InterlockSocket, type InterlockConfig, type PeerInfo } from './socket.js';
export { encode, decode, SIGNAL_TYPES, getSignalName, type DecodedMessage } from './protocol.js';
export { Tumbler } from './tumbler.js';
export { SignalHandlers, type SignalHandler } from './handlers.js';
