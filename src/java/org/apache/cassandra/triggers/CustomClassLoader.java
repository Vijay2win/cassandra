package org.apache.cassandra.triggers;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOError;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

/**
 * Custom class loader will load the classes from the class path, CCL will load
 * the classes from the the URL first, if it cannot find the required class it
 * will let the parent class loader do the its job.
 *
 * Note: If the CCL is GC'ed then the associated classes will be unloaded.
 */
public class CustomClassLoader extends URLClassLoader
{
    private static final Logger logger = LoggerFactory.getLogger(CustomClassLoader.class);
    private final Map<String, Class<?>> cache = new ConcurrentHashMap<String, Class<?>>();
    private final ClassLoader parent;

    public CustomClassLoader(ClassLoader parent)
    {
        super(new URL[] {}, parent);
        assert parent != null;
        this.parent = getParent();
    }

    public CustomClassLoader(ClassLoader parent, File classPathDir)
    {
        super(new URL[] {}, parent);
        assert parent != null;
        this.parent = getParent();
        addClassPath(classPathDir);
    }

    public void addClassPath(File dir)
    {
        if (dir == null || !dir.exists())
            return;
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(".jar");
            }
        };
        for (File inputJar : dir.listFiles(filter))
        {
            File lib = new File(System.getProperty("java.io.tmpdir"), "lib");
            if (!lib.exists())
            {
                lib.mkdir();
                lib.deleteOnExit();
            }
            try
            {
                File out = File.createTempFile("cassandra-", ".jar", lib);
                out.deleteOnExit();
                logger.info("Loading new jar {}", inputJar.getAbsolutePath());
                Files.copy(inputJar, out);
                addURL(out.toURL());
            }
            catch (IOException ex)
            {
                throw new IOError(ex);
            }
        }
    }

    public Class<?> loadClass(String name) throws ClassNotFoundException
    {
        Class<?> clazz = cache.get(name);
        if (clazz == null)
            return loadClassInternal(name);
        return clazz;
    }

    public synchronized Class<?> loadClassInternal(String name) throws ClassNotFoundException
    {
        ClassNotFoundException ex = null;
        Class<?> clazz = null;
        try
        {
            clazz = this.findClass(name);
        }
        catch (ClassNotFoundException e)
        {
            ex = e;
        }
        if (clazz == null)
            clazz = parent.loadClass(name);
        if (clazz == null)
            throw ex;
        cache.put(name, clazz);
        return clazz;
    }
}
