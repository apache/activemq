using System;

namespace OpenWire.Core
{
	/// <summary>
	/// Summary description for TransactionConstants.
	/// </summary>
	public class TransactionType 
	{
    
		/**
		 * Transaction state not set
		 */
		const int NOT_SET = 0;
		/**
		 * Start a transaction
		 */
		const int START = 101;
		/**
		 * Pre-commit a transaction
		 */
		const int PRE_COMMIT = 102;
		/**
		 * Commit a transaction
		 */
		const int COMMIT = 103;
		/**
		 * Recover a transaction
		 */
		const int RECOVER = 104;
		/**
		 * Rollback a transaction
		 */
		const int ROLLBACK = 105;
		/**
		 * End a transaction
		 */
		const int END = 106;
		/**
		 * Forget a transaction
		 */
		const int FORGET = 107;
		/**
		 * Join a transaction
		 */
		const int JOIN = 108;
		/**
		 * Do a one phase commit...  No PRE COMMIT has been done.
		 */
		const int COMMIT_ONE_PHASE = 109;
		/**
		 * Get a list of all the XIDs that are currently prepared.
		 */
		const int XA_RECOVER = 110;
		/**
		 * Get a the transaction timeout for the RM
		 */
		const int GET_TX_TIMEOUT = 111;
		/**
		 * Set a the transaction timeout for the RM
		 */
		const int SET_TX_TIMEOUT = 112;
		/**
		 * Gets the unique id of the resource manager.
		 */
		const int GET_RM_ID = 113;
	}
}
