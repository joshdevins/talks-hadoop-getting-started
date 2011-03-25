package net.joshdevins.talks.hadoopstart.pig.scripts;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.pig.pigunit.PigTest;
import org.junit.After;

/**
 * A simple base class to sort out quirks in PigUnit mostly to do with data cleanup and storage. This is NOT thread safe
 * and assume only one test is using it at any one time!
 * 
 * @author Josh Devins
 */
public abstract class PigUnitBase {

    private static final Logger LOG = Logger.getLogger(PigUnitBase.class);

    public static final String DEFAULT_BASE_DATA_DIR = "target/data";

    public static final String INPUT_DATA_FILENAME = "pigunit-input-overriden.txt";

    public static final String INPUT_DATA_CRC_FILENAME = "." + INPUT_DATA_FILENAME + ".crc";

    private final LinkedList<Path> dataPathsAdded;

    private final String baseDataDir;

    private final String testClassName;

    protected PigUnitBase(final Class<? extends PigUnitBase> testClass) {
        this(DEFAULT_BASE_DATA_DIR, testClass);
    }

    protected PigUnitBase(final String baseDataDir, final Class<? extends PigUnitBase> testClass) {

        Validate.notNull(baseDataDir);
        Validate.notNull(testClass);

        this.baseDataDir = baseDataDir;
        this.testClassName = testClass.getSimpleName();
        this.dataPathsAdded = new LinkedList<Path>();
    }

    @After
    public void after() throws Exception {

        // cleanup crap that might be left over from PigUnit
        deleteFileOrDirectory(buildBaseDataPathString());
        deleteFileOrDirectory(INPUT_DATA_FILENAME);
        deleteFileOrDirectory(INPUT_DATA_CRC_FILENAME);

        // remove anything copied into the fake cluster filesystem
        for (Path path : dataPathsAdded) {
            PigTest.getCluster().delete(path);
        }

        dataPathsAdded.clear();
    }

    /**
     * Does a copy from the local filesystem into the fake cluster filesystem. This will place the file into the
     * directory defined with the constructor or {@link #DEFAULT_BASE_DATA_DIR}.
     * 
     * @param localFilesystemPath
     *        path to the file on the local filesystem
     * @param fakeFilesystemPath
     *        name that the file should take in the fake cluster filesystem, any directories are relative and appended
     *        to what has been set in {@link #setBaseDataDir(String)}
     */
    protected void addDataToCluster(final String localFilesystemPath, final String fakeFilesystemPath)
            throws IOException {

        Path path = buildInputPath(fakeFilesystemPath);
        dataPathsAdded.add(path);
        PigTest.getCluster().copyFromLocalFile(new Path(localFilesystemPath), path);
    }

    protected String buildInputPathString(final String pathSuffix) {
        return buildBaseDataPathString() + "/input/" + pathSuffix;
    }

    protected String buildOutputPathString() {
        return buildBaseDataPathString() + "/output";
    }

    private String buildBaseDataPathString() {
        return baseDataDir + "/" + testClassName;
    }

    private Path buildInputPath(final String pathSuffix) {
        return new Path(buildInputPathString(pathSuffix));
    }

    private static boolean deleteDir(final File dir) {

        if (dir.exists()) {
            File[] files = dir.listFiles();

            for (File file : files) {

                if (file.isDirectory()) {
                    deleteDir(file);
                } else {

                    boolean deleted = file.delete();
                    if (!deleted) {
                        // don't fail the test, just log something
                        LOG.warn("Could not delete file in directory to be deleted: " + dir.toString());
                    }
                }
            }
        }

        boolean deleted = dir.delete();
        if (!deleted) {
            // don't fail the test, just log something
            LOG.warn("Could not delete directory: " + dir.toString());
        }

        return deleted;
    }

    private static void deleteFileOrDirectory(final String filename) {

        File file = new File(filename);

        if (!file.exists()) {
            return;
        }

        if (file.isDirectory()) {
            deleteDir(file);
            return;
        }

        boolean deleted = file.delete();
        if (!deleted) {
            // don't fail the test, just log something
            LOG.warn("Could not delete file: " + filename);
        }
    }
}
