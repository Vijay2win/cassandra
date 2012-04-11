package org.apache.cassandra.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class OptimizedStreamsTest
{

    @Test
    public void testStreams() throws IOException
    {
        
        ByteArrayOutputStream byteArrayOStream1 = new ByteArrayOutputStream();
        OptimizedDataOutput odos = new OptimizedDataOutput(new DataOutputStream(byteArrayOStream1));
        
        ByteArrayOutputStream byteArrayOStream2 = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(byteArrayOStream2);
        
        for (int i = 0; i < 10000; i++)
        {
            dos.writeInt(i);
            odos.writeInt(i);
        }
        dos.flush();
        System.out.println(byteArrayOStream1.size());
        System.out.println(byteArrayOStream2.size());
        
        for (long i = Integer.MAX_VALUE; i < ((long)Integer.MAX_VALUE + 10000); i++)
        {
            dos.writeLong(i);
            odos.writeLong(i);
        }
        dos.flush();
        System.out.println(byteArrayOStream1.size());
        System.out.println(byteArrayOStream2.size());
        Assert.assertTrue(byteArrayOStream1.size() < byteArrayOStream2.size());
        
        ByteArrayInputStream byteArrayIStream1 = new ByteArrayInputStream(byteArrayOStream1.toByteArray());
//        OptimizedDataInput idis = new OptimizedDataInput(new DataInputStream(byteArrayIStream1));        
//        for (int i = 0; i < 10000; i++)
//        {
//            Assert.assertEquals(i, idis.readInt());
//        }
//        
//        for (int i = Integer.MAX_VALUE; i < Integer.MAX_VALUE + 1000; i++)
//        {
//            Assert.assertEquals(i, idis.readInt());
//        }
    }
}
