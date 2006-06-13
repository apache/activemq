/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.maven;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.deployer.ArtifactDeployer;
import org.apache.maven.artifact.deployer.ArtifactDeploymentException;
import org.apache.maven.artifact.repository.ArtifactRepository;
import org.apache.maven.artifact.repository.DefaultArtifactRepository;
import org.apache.maven.artifact.repository.layout.ArtifactRepositoryLayout;
import org.apache.maven.artifact.repository.layout.DefaultRepositoryLayout;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;

/**
 * This is a useful little mojo that allows you to copy all the transitive
 * dependencies to a specified deployment repository.  Great if you want to create
 * a new repo for your project's dependencies.
 * 
 * @goal deploy-dependencies
 * @requiresDependencyResolution compile
 * @phase process-sources
 */
public class DeployDependenciesMojo extends AbstractMojo {

    /**
     * @parameter expression="${project}"
     * @readonly
     * @required
     */
    private MavenProject project;

    /**
     * @parameter expression="${reactorProjects}"
     * @required
     * @readonly
     */
    private List reactorProjects;

    /**
     * @parameter expression="${component.org.apache.maven.artifact.deployer.ArtifactDeployer}"
     * @required
     * @readonly
     */
    private ArtifactDeployer deployer;

    /**
     * @parameter expression="${localRepository}"
     * @required
     * @readonly
     */
    private ArtifactRepository localRepository;

    /**
     * @parameter
     */
    private ArtifactRepository deploymentRepository;
    
    /**
     * @parameter
     */
    private String deploymentRepositoryId;
    
    /**
     * @parameter
     */
    private String deploymentRepositoryUrl;

    /**
     * @parameter
     */
    private ArtifactRepositoryLayout deploymentRepositoryLayout = new DefaultRepositoryLayout();
    
    
    /**
     * 
     */
    public void execute() throws MojoExecutionException {
        
        if ( deploymentRepository == null )
        {
            deploymentRepository = new DefaultArtifactRepository(deploymentRepositoryId, deploymentRepositoryUrl, deploymentRepositoryLayout); 
        }
        

        getLog().info("repo type="+deploymentRepository.getClass());
        
        String protocol = deploymentRepository.getProtocol();
        
        if( protocol.equals( "scp" ) )
        {
                File sshFile = new File( System.getProperty( "user.home" ), ".ssh" );

                if( !sshFile.exists() )
                {
                        sshFile.mkdirs();
                }   
        }
        
        Set dependencies = new HashSet();

        for (Iterator i = reactorProjects.iterator(); i.hasNext();) {
            MavenProject p = (MavenProject) i.next();
            List dependencyArtifacts = p.getTestArtifacts();
            
            if( dependencyArtifacts !=null )
                dependencies.addAll(dependencyArtifacts);
        }

        for (Iterator iter = dependencies.iterator(); iter.hasNext();) {
            Artifact artifact = (Artifact) iter.next();
            deploy( artifact );
        }
        
    }

    private void deploy(Artifact artifact) throws MojoExecutionException {
        try
        {
            getLog().info( "Copying " + artifact.getFile().getAbsolutePath() + " to " + deploymentRepository );            
            deployer.deploy( artifact.getFile(), artifact, deploymentRepository, localRepository );
        }
        catch ( ArtifactDeploymentException e )
        {
            throw new MojoExecutionException( e.getMessage(), e );
        }
    }


}
