package org.apache.activemq.maven;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;
import org.codehaus.plexus.archiver.ArchiverException;
import org.codehaus.plexus.archiver.UnArchiver;
import org.codehaus.plexus.archiver.manager.ArchiverManager;
import org.codehaus.plexus.archiver.manager.NoSuchArchiverException;
import org.codehaus.plexus.util.FileUtils;

/**
 * @goal createbundle
 * @description Creates an xfire bundle
 */
public class BundleMojo extends AbstractMojo 
{
    /**
     * The output directory of the assembled distribution file.
     *
     * @parameter expression="${project.build.outputDirectory}"
     * @required
     */
    protected File outputDirectory;
    
    /**
     * Inclusion list
     *
     * @parameter
     */
    String includes = "";
    
    /**
     * The Maven Project.
     *
     * @parameter expression="${project}"
     * @required
     * @readonly
     */
    MavenProject project;
    
    /**
     * To look up Archiver/UnArchiver implementations
     *
     * @parameter expression="${component.org.codehaus.plexus.archiver.manager.ArchiverManager}"
     * @required
     */
    protected ArchiverManager archiverManager;
    
    public void execute() throws MojoExecutionException 
    {
        String[] include = includes.split(",");
        List includeList = Arrays.asList(include);
        getLog().info("Inclusions: " + includeList);
        getLog().info("OutputDirectory: " + outputDirectory);
        outputDirectory.mkdirs();
        
        for (Iterator itr = project.getArtifacts().iterator(); itr.hasNext();)
        {
            Artifact a = (Artifact) itr.next();

            if (includeList.contains(a.getArtifactId()))
            {
                getLog().info("Found " + a.getArtifactId());
                
                try 
                {
                    unpack( a.getFile(), outputDirectory );
                } 
                catch (MojoExecutionException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } 
                catch (NoSuchArchiverException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }
    
    protected void unpack( File file, File location )
        throws MojoExecutionException, NoSuchArchiverException
    {
        String archiveExt = FileUtils.getExtension( file.getAbsolutePath() ).toLowerCase();
    
        try
        {
            UnArchiver unArchiver = this.archiverManager.getUnArchiver( archiveExt );
    
            unArchiver.setSourceFile( file );
    
            unArchiver.setDestDirectory( location );
    
            unArchiver.extract();
        }
        catch ( IOException e )
        {
            throw new MojoExecutionException( "Error unpacking file: " + file + "to: " + location, e );
        }
        catch ( ArchiverException e )
        {
            throw new MojoExecutionException( "Error unpacking file: " + file + "to: " + location, e );
        }
    }
}
