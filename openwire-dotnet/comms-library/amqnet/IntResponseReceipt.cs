using System;

namespace ActiveMQ
{
    /// <summary>
    /// Summary description for IntResponseReceipt.
    /// </summary>
    public class IntResponseReceipt : Receipt {

        private int result;

        /**
         * @see org.activemq.message.Receipt#getPacketType()
         */
        public new int getPacketType() 
        {
            return INT_RESPONSE_RECEIPT_INFO;
        }

        /**
         * @return Returns the result.
         */
        public int getResult() 
        {
            return result;
        }

        /**
         * @param result The result to set.
         */
        public void setResult(int result) 
        {
            this.result = result;
        }

        public new String ToString() 
        {
            return super.toString() + " IntResponseReceipt{ " +
                "result = " + result +
                " }";
        }
    }
}
