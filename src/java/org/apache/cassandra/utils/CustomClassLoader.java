package org.apache.cassandra.utils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class CustomClassLoader extends URLClassLoader
{
    private static final Logger logger = LoggerFactory.getLogger(CustomClassLoader.class);
    private final ClassLoader parent;

    public CustomClassLoader(ClassLoader parent)
    {
        super(new URL[] {}, parent != null ? parent : (Thread.currentThread().getContextClassLoader() != null ? Thread.currentThread().getContextClassLoader() : (CustomClassLoader.class
                .getClassLoader() != null ? CustomClassLoader.class.getClassLoader() : ClassLoader.getSystemClassLoader())));
        this.parent = getParent();
        assert parent != null;
    }

    public void addClassPath(String classPath) throws IOException
    {
        if (classPath == null)
            return;
        StringTokenizer tokenizer = new StringTokenizer(classPath, ",;");
        while (tokenizer.hasMoreTokens())
        {
            File dir = new File(tokenizer.nextToken());
            for (File in : dir.listFiles())
            {

                File tmp_dir = new File(System.getProperty("java.io.tmpdir"));
                File lib = new File(tmp_dir, "lib");
                if (!lib.exists())
                {
                    lib.mkdir();
                    lib.deleteOnExit();
                }
                File out = File.createTempFile("cassandra-", ".jar", lib);
                out.deleteOnExit();
                if (logger.isDebugEnabled())
                    logger.debug("Extract " + in.getAbsolutePath() + " to " + out);
                Files.copy(in, out);
                addURL(out.toURL());
            }
        }
    }

    public Class loadClass(String name) throws ClassNotFoundException
    {
        return loadClass(name, false);
    }

    protected synchronized Class loadClass(String name, boolean resolve) throws ClassNotFoundException
    {
        Class c = findLoadedClass(name);
        ClassNotFoundException ex = null;
        if (c == null)
        {
            try
            {
                c = this.findClass(name);
            }
            catch (ClassNotFoundException e)
            {
                ex = e;
            }
        }
        if (c == null && parent != null)
            c = parent.loadClass(name);
        if (c == null)
            throw ex;
        if (resolve)
            resolveClass(c);
        if (logger.isDebugEnabled())
            logger.debug("loaded " + c + " from " + c.getClassLoader());
        return c;
    }

    @Override
    public String toString()
    {
        return "Custom Class Loader for (" + StringUtils.join(getURLs()) + ") / " + parent;
    }
}
