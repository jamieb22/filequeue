package rules;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.rules.MethodRule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import java.nio.file.FileSystem;

/**
 * Created by Valentin Popov popov@mailarchiva.ru on 25/04/2018.
 */
public class JimfsFilesystemRule implements MethodRule, TestRule {

    private final Configuration config;
    private FileSystem jimfs;

    /**
     * Build {@link FileSystem} with default {@link Configuration#unix()}
     */
    public JimfsFilesystemRule() {
          this.config = Configuration.unix();
    }

    /**
     * Build {@link FileSystem} with provided {@link Configuration}
     */
    public JimfsFilesystemRule(Configuration configuration) {
        this.config = configuration;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                jimfs = Jimfs.newFileSystem(config);
                try {
                    base.evaluate();
                } finally {
                    jimfs.close();
                }
            }

        };
    }

    @Override
    public Statement apply(final Statement base, FrameworkMethod frameworkMethod, Object o) {
        return apply(base, null, null);
    }

    public FileSystem getFileSystem() {
        return jimfs;
    }

}
