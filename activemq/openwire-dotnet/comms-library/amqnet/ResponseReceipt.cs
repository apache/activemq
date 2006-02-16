using System;

namespace ActiveMQ
{
    /// <summary>
    /// Summary description for ResponseReceipt.
    /// </summary>
public class ResponseReceipt : Receipt {

    private Object result;
    private byte[] resultBytes;

    /**
     * @see org.activemq.message.Receipt#getPacketType()
     */
    public new int getPacketType() {
        return RESPONSE_RECEIPT_INFO;
    }

    /**
     * @return Returns the result.
     */
    public Object getResult() {
        return null;
        //TODO Serilization code
    }

    /**
     * @param result The result to set.
     */
    public void setResult(Object result) {
        this.result = result;
        this.resultBytes = null;
    }

    /**
     * @param data
     */
    public void setResultBytes(byte[] resultBytes) {
        this.resultBytes = resultBytes;
        this.result = null;
    }

    /**
     * @return Returns the resultBytes.
     */
    public byte[] getResultBytes()  {

        //TODO SERILIZATION
        return null;
    }

    public override String ToString() {
        return base.toString() + " ResponseReceipt{ " +
                "result = " + result +
                ", resultBytes = " + resultBytes +
                " }";
    }
}

}
