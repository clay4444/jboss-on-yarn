/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yarnbook;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 * JBoss AS 7.1 Application Master. Spin up domain mode (i.e. clustered)
 * services via YARN.
 */

public class JBossApplicationMaster {

    private static final Logger LOG = Logger
            .getLogger(JBossApplicationMaster.class.getName());

    private Configuration conf;

    @SuppressWarnings("rawtypes")
    private AMRMClientAsync resourceManager;  //负责和ResourceManager进行通信，以此来提供异步更新事件，比如容器分配和完成，内部通过一个线程周期性的和RM进行通信；
    private NMClientAsync nmClientAsync;  //负责和NodeManager进行通信，得到响应，以此来提供异步更新事件，内部包含一个线程池，每个线程通过使用${NMClientImpl}与每个独立的NodeManager进行通信，线程池大小可配置；
    private NMCallbackHandler containerListener;

    private ApplicationAttemptId appAttemptID;

    private String appMasterHostname = "";
    private int appMasterRpcPort = 0;
    private String appMasterTrackingUrl = "";

    private int numTotalContainers = 2;  //container 个数
    private int containerMemory = 1024;  //单个container的内存大小
    private int requestPriority;    //优先级

    private String adminUser;           //adminUser
    private String adminPassword;      //adminPassword

    private AtomicInteger numCompletedContainers = new AtomicInteger(); //完成的container个数
    private AtomicInteger numAllocatedContainers = new AtomicInteger();  //已分配的container个数
    private AtomicInteger numFailedContainers = new AtomicInteger();    //失败的container个数
    private AtomicInteger numRequestedContainers = new AtomicInteger(); //已请求的container个数

    private Map<String, String> shellEnv = new HashMap<String, String>();

    private String jbossHome;
    private String appJar;
    private String domainController;

    private volatile boolean done;
    private volatile boolean success;

    private List<Thread> launchThreads = new ArrayList<Thread>();  //多个线程？

    /**
     * @param args
     *            Command line args
     */
    public static void main(String[] args) {
        boolean result = false;
        try {
            JBossApplicationMaster appMaster = new JBossApplicationMaster();  //给conf 赋值，直接new，从环境变量找；
            LOG.info("Initializing JBossApplicationMaster");
            boolean doRun = appMaster.init(args); //解析命令行参数，并赋给当前的全局变量
            if (!doRun) {
                System.exit(0);
            }
            result = appMaster.run();  //run方法
        } catch (Throwable t) {
            LOG.log(Level.SEVERE, "Error running JBossApplicationMaster", t);
            System.exit(1);
        }
        if (result) {
            LOG.info("Application Master completed successfully. exiting");
            System.exit(0);
        } else {
            LOG.info("Application Master failed. exiting");
            System.exit(2);
        }
    }

    /**
     * Dump out contents of $CWD and the environment to stdout for debugging
     * debug模式下，打印CMD路径的所有环境变量和文件；
     */
    private void dumpOutDebugInfo() {

        LOG.info("Dump debug output");
        Map<String, String> envs = System.getenv();  //打印所有环境变量
        for (Map.Entry<String, String> env : envs.entrySet()) {
            LOG.info("System env: key=" + env.getKey() + ", val="
                    + env.getValue());
            System.out.println("System env: key=" + env.getKey() + ", val="
                    + env.getValue());
        }

        String cmd = "ls -al";               //打印当前执行命令的文件夹的所有文件；
        Runtime run = Runtime.getRuntime();
        Process pr = null;
        try {
            pr = run.exec(cmd);
            pr.waitFor();

            BufferedReader buf = new BufferedReader(new InputStreamReader(
                    pr.getInputStream()));
            String line = "";
            while ((line = buf.readLine()) != null) {
                LOG.info("System CWD content: " + line);
                System.out.println("System CWD content: " + line);
            }
            buf.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public JBossApplicationMaster() throws Exception {
        conf = new YarnConfiguration();
    }

    /**
     * Parse command line options
     *
     * @param args
     *            Command line args
     * @return Whether init successful and run should be invoked
     * @throws ParseException
     * @throws IOException
     */
    public boolean init(String[] args) throws ParseException, IOException {

        //这些参数都是在JBossClient设置CLC的container启动命令的时候赋值过来的；
        Options opts = new Options();
        opts.addOption("app_attempt_id", true,
                "App Attempt ID. Not to be used unless for testing purposes");//测试的时候使用；
        opts.addOption("admin_user", true,
                "User id for initial administrator user");
        opts.addOption("admin_password", true,
                "Password for initial administrator user");
        opts.addOption("container_memory", true,
                "Amount of memory in MB to be requested to run the shell command");
        opts.addOption("num_containers", true,
                "No. of containers on which the shell command needs to be executed");
        opts.addOption("jar", true, "JAR file containing the application");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("debug", false, "Dump out debug information");

        opts.addOption("help", false, "Print usage");
        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            printUsage(opts);
            throw new IllegalArgumentException(
                    "No args specified for application master to initialize");
        }

        if (cliParser.hasOption("help")) {  //查看运行参数列表
            printUsage(opts);
            return false;
        }

        if (cliParser.hasOption("debug")) {  //debug模式
            dumpOutDebugInfo();
        }

        Map<String, String> envs = System.getenv();  //获取环境变量

        ContainerId containerId = ConverterUtils.toContainerId(envs
                .get(Environment.CONTAINER_ID.name()));

        if (!envs.containsKey(Environment.CONTAINER_ID.name())) {  //appAttemptID 赋值
            if (cliParser.hasOption("app_attempt_id")) {
                String appIdStr = cliParser
                        .getOptionValue("app_attempt_id", "");
                appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
            } else {
                throw new IllegalArgumentException(
                        "Application Attempt Id not set in the environment");
            }
        } else {
            containerId = ConverterUtils.toContainerId(envs
                    .get(Environment.CONTAINER_ID.name()));
            appAttemptID = containerId.getApplicationAttemptId();
        }

        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {  //The environment variable for APP_SUBMIT_TIME. Set in AppMaster environment only
            throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HOST.name())) {  //NodeManager host地址
            throw new RuntimeException(Environment.NM_HOST.name()
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {  //NodeManager  http 端口
            throw new RuntimeException(Environment.NM_HTTP_PORT
                    + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_PORT.name())) {    //NodeManager 端口
            throw new RuntimeException(Environment.NM_PORT.name()
                    + " not set in the environment");
        }

        LOG.info("Application master for app" + ", appId="
                + appAttemptID.getApplicationId().getId()
                + ", clustertimestamp="
                + appAttemptID.getApplicationId().getClusterTimestamp()
                + ", attemptId=" + appAttemptID.getAttemptId());

        containerMemory = Integer.parseInt(cliParser.getOptionValue(  //containerMemory
                "container_memory", "1024"));
        numTotalContainers = Integer.parseInt(cliParser.getOptionValue(  //numTotalContainers
                "num_containers", "1"));
        adminUser = cliParser.getOptionValue("admin_user", "yarn");   //adminUser
        adminPassword = cliParser.getOptionValue("admin_password", "yarn");  //adminPassword
        appJar = cliParser.getOptionValue("jar");                                   //appJar
        if (numTotalContainers == 0) {
            throw new IllegalArgumentException(
                    "Cannot run JBoss Application Master with no containers");
        }
        requestPriority = Integer.parseInt(cliParser.getOptionValue("priority", //requestPriority
                "0"));

        return true;
    }

    /**
     * Helper function to print usage
     *
     * @param opts
     *            Parsed command line options
     */
    private void printUsage(Options opts) {
        new HelpFormatter().printHelp("JBossApplicationMaster", opts);
    }

    /**
     * AppMaster 真正的执行方法，
     * Main run function for the application master
     * @throws YarnException
     * @throws IOException
     */
    @SuppressWarnings({ "unchecked" })
    public boolean run() throws YarnException, IOException {
        LOG.info("Starting JBossApplicationMaster");

        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        resourceManager = AMRMClientAsync.createAMRMClientAsync(1000,
                allocListener);
        resourceManager.init(conf);
        resourceManager.start();

        containerListener = new NMCallbackHandler();
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        RegisterApplicationMasterResponse response = resourceManager
                .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
                        appMasterTrackingUrl);

        int maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        // A resource ask cannot exceed the max.
        if (containerMemory > maxMem) {
            LOG.info("Container memory specified above max threshold of cluster."
                    + " Using max value."
                    + ", specified="
                    + containerMemory
                    + ", max=" + maxMem);
            containerMemory = maxMem;
        }

        for (int i = 0; i < numTotalContainers; ++i) {
            ContainerRequest containerAsk = setupContainerAskForRM();
            resourceManager.addContainerRequest(containerAsk);
        }
        numRequestedContainers.set(numTotalContainers);

        while (!done) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
            }
        }
        finish();

        return success;
    }

    private void finish() {
        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(10000);
            } catch (InterruptedException e) {
                LOG.info("Exception thrown in thread join: " + e.getMessage());
                e.printStackTrace();
            }
        }

        LOG.info("Application completed. Stopping running containers");
        nmClientAsync.stop();

        LOG.info("Application completed. Signalling finish to RM");

        FinalApplicationStatus appStatus;
        String appMessage = null;
        success = true;
        if (numFailedContainers.get() == 0
                && numCompletedContainers.get() == numTotalContainers) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics." + ", total=" + numTotalContainers
                    + ", completed=" + numCompletedContainers.get()
                    + ", allocated=" + numAllocatedContainers.get()
                    + ", failed=" + numFailedContainers.get();
            success = false;
        }
        try {
            resourceManager.unregisterApplicationMaster(appStatus, appMessage,
                    null);
        } catch (YarnException ex) {
            LOG.log(Level.SEVERE, "Failed to unregister application", ex);
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "Failed to unregister application", e);
        }

        done = true;
        resourceManager.stop();
    }

    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        @SuppressWarnings("unchecked")
        public void onContainersCompleted(
                List<ContainerStatus> completedContainers) {
            LOG.info("Got response from RM for container ask, completedCnt="
                    + completedContainers.size());
            for (ContainerStatus containerStatus : completedContainers) {
                LOG.info("Got container status for containerID="
                        + containerStatus.getContainerId() + ", state="
                        + containerStatus.getState() + ", exitStatus="
                        + containerStatus.getExitStatus() + ", diagnostics="
                        + containerStatus.getDiagnostics());

                assert (containerStatus.getState() == ContainerState.COMPLETE);

                int exitStatus = containerStatus.getExitStatus();
                if (0 != exitStatus) {
                    if (ContainerExitStatus.ABORTED != exitStatus) {
                        numCompletedContainers.incrementAndGet();
                        numFailedContainers.incrementAndGet();
                    } else {
                        numAllocatedContainers.decrementAndGet();
                        numRequestedContainers.decrementAndGet();
                    }
                } else {
                    numCompletedContainers.incrementAndGet();
                    LOG.info("Container completed successfully."
                            + ", containerId="
                            + containerStatus.getContainerId());
                }
            }

            int askCount = numTotalContainers - numRequestedContainers.get();
            numRequestedContainers.addAndGet(askCount);

            if (askCount > 0) {
                for (int i = 0; i < askCount; ++i) {
                    ContainerRequest containerAsk = setupContainerAskForRM();
                    resourceManager.addContainerRequest(containerAsk);
                }
            }

            if (numCompletedContainers.get() == numTotalContainers) {
                done = true;
            }
        }

        public void onContainersAllocated(List<Container> allocatedContainers) {
            LOG.info("Got response from RM for container ask, allocatedCnt="
                    + allocatedContainers.size());
            numAllocatedContainers.addAndGet(allocatedContainers.size());
            for (Container allocatedContainer : allocatedContainers) {
                LOG.info("Launching shell command on a new container."
                        + ", containerId=" + allocatedContainer.getId()
                        + ", containerNode="
                        + allocatedContainer.getNodeId().getHost() + ":"
                        + allocatedContainer.getNodeId().getPort()
                        + ", containerNodeURI="
                        + allocatedContainer.getNodeHttpAddress()
                        + ", containerResourceMemory"
                        + allocatedContainer.getResource().getMemory());

                LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(
                        allocatedContainer, containerListener);
                Thread launchThread = new Thread(runnableLaunchContainer);

                launchThreads.add(launchThread);
                launchThread.start();
            }
        }

        public void onShutdownRequest() {
            done = true;
        }

        public void onNodesUpdated(List<NodeReport> updatedNodes) {
        }

        public float getProgress() {
            float progress = (float) numCompletedContainers.get()
                    / numTotalContainers;
            return progress;
        }

        public void onError(Throwable e) {
            done = true;
            resourceManager.stop();
        }
    }

    private class NMCallbackHandler implements NMClientAsync.CallbackHandler {

        private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();

        public void addContainer(ContainerId containerId, Container container) {
            containers.putIfAbsent(containerId, container);
            LOG.info("Callback container id : " + containerId.toString());

            if (containers.size() == 1) {
                domainController = container.getNodeId().getHost();
            }
        }

        public void onContainerStopped(ContainerId containerId) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Succeeded to stop Container " + containerId);
            }
            containers.remove(containerId);
        }

        public void onContainerStatusReceived(ContainerId containerId,
                                              ContainerStatus containerStatus) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Container Status: id=" + containerId + ", status="
                        + containerStatus);
            }
        }

        public void onContainerStarted(ContainerId containerId,
                                       Map<String, ByteBuffer> allServiceResponse) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Succeeded to start Container " + containerId);
            }
            Container container = containers.get(containerId);
            if (container != null) {
                nmClientAsync.getContainerStatusAsync(containerId,
                        container.getNodeId());
            }
        }

        public void onStartContainerError(ContainerId containerId, Throwable t) {
            LOG.log(Level.SEVERE, "Failed to start Container " + containerId);
            containers.remove(containerId);
        }

        public void onGetContainerStatusError(ContainerId containerId,
                                              Throwable t) {
            LOG.log(Level.SEVERE, "Failed to query the status of Container "
                    + containerId);
        }

        public void onStopContainerError(ContainerId containerId, Throwable t) {
            LOG.log(Level.SEVERE, "Failed to stop Container " + containerId);
            containers.remove(containerId);
        }

        public int getContainerCount() {
            return containers.size();
        }
    }

    /**
     * Thread to connect to the {@link ContainerManagementProtocol} and launch
     * the container that will execute the shell command.
     */
    private class LaunchContainerRunnable implements Runnable {

        Container container;

        NMCallbackHandler containerListener;

        /**
         * @param lcontainer
         *            Allocated container
         * @param containerListener
         *            Callback handler of the container
         */
        public LaunchContainerRunnable(Container lcontainer,
                                       NMCallbackHandler containerListener) {
            this.container = lcontainer;
            this.containerListener = containerListener;
        }

        /**
         * Connects to CM, sets up container launch context for shell command
         * and eventually dispatches the container start request to the CM.
         */
        public void run() {

            String containerId = container.getId().toString();

            LOG.info("Setting up container launch container for containerid="
                    + container.getId());
            ContainerLaunchContext ctx = Records
                    .newRecord(ContainerLaunchContext.class);

            ctx.setEnvironment(shellEnv);

            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

            String applicationId = container.getId().getApplicationAttemptId()
                    .getApplicationId().toString();
            try {
                FileSystem fs = FileSystem.get(conf);

                LocalResource jbossDist = Records
                        .newRecord(LocalResource.class);
                jbossDist.setType(LocalResourceType.ARCHIVE);
                jbossDist.setVisibility(LocalResourceVisibility.APPLICATION);

                Path jbossDistPath = new Path(new URI(
                        JBossConstants.JBOSS_DIST_PATH));
                jbossDist.setResource(ConverterUtils
                        .getYarnUrlFromPath(jbossDistPath));

                jbossDist.setTimestamp(fs.getFileStatus(jbossDistPath)
                        .getModificationTime());
                jbossDist.setSize(fs.getFileStatus(jbossDistPath).getLen());
                localResources.put(JBossConstants.JBOSS_SYMLINK, jbossDist);

                LocalResource jbossConf = Records
                        .newRecord(LocalResource.class);
                jbossConf.setType(LocalResourceType.FILE);
                jbossConf.setVisibility(LocalResourceVisibility.APPLICATION);

                Path jbossConfPath = new Path(new URI(appJar));
                jbossConf.setResource(ConverterUtils
                        .getYarnUrlFromPath(jbossConfPath));

                jbossConf.setTimestamp(fs.getFileStatus(jbossConfPath)
                        .getModificationTime());
                jbossConf.setSize(fs.getFileStatus(jbossConfPath).getLen());
                localResources.put(JBossConstants.JBOSS_ON_YARN_APP, jbossConf);

            } catch (Exception e) {
                LOG.log(Level.SEVERE, "Problem setting local resources", e);
                numCompletedContainers.incrementAndGet();
                numFailedContainers.incrementAndGet();
                return;
            }

            ctx.setLocalResources(localResources);

            List<String> commands = new ArrayList<String>();

            String host = container.getNodeId().getHost();

            String containerHome = conf.get("yarn.nodemanager.local-dirs")
                    + File.separator + ContainerLocalizer.USERCACHE
                    + File.separator
                    + System.getenv().get(Environment.USER.toString())
                    + File.separator + ContainerLocalizer.APPCACHE
                    + File.separator + applicationId + File.separator
                    + containerId;
            jbossHome = containerHome + File.separator
                    + JBossConstants.JBOSS_SYMLINK + File.separator
                    + JBossConstants.JBOSS_VERSION;

            String jbossPermissionsCommand = String.format("chmod -R 777 %s",
                    jbossHome);

            int portOffset = 0;
            int containerCount = containerListener.getContainerCount();
            if (containerCount > 1) {
                portOffset = containerCount * 150;
            }

            String domainControllerValue;
            if (domainController == null) {
                domainControllerValue = host;
            } else {
                domainControllerValue = domainController;
            }

            String jbossConfigurationCommand = String
                    .format("%s/bin/java -cp %s %s --home %s --server_group %s --server %s --port_offset %s --admin_user %s --admin_password %s --domain_controller %s --host %s",
                            Environment.JAVA_HOME.$(),
                            "/opt/hadoop-2.1.0-beta/share/hadoop/common/lib/*"
                                    + File.pathSeparator + containerHome
                                    + File.separator
                                    + JBossConstants.JBOSS_ON_YARN_APP,
                            JBossConfiguration.class.getName(), jbossHome,
                            applicationId, containerId, portOffset, adminUser,
                            adminPassword, domainControllerValue, host);

            LOG.info("Configuring JBoss on " + host + " with: "
                    + jbossConfigurationCommand);

            String jbossCommand = String
                    .format("%s%sbin%sdomain.sh -Djboss.bind.address=%s -Djboss.bind.address.management=%s -Djboss.bind.address.unsecure=%s",
                            jbossHome, File.separator, File.separator, host,
                            host, host);

            LOG.info("Starting JBoss with: " + jbossCommand);

            commands.add(jbossPermissionsCommand);
            commands.add(JBossConstants.COMMAND_CHAIN);
            commands.add(jbossConfigurationCommand);
            commands.add(JBossConstants.COMMAND_CHAIN);
            commands.add(jbossCommand);

            ctx.setCommands(commands);

            containerListener.addContainer(container.getId(), container);
            nmClientAsync.startContainerAsync(container, ctx);
        }
    }

    /**
     * Setup the request that will be sent to the RM for the container ask.
     *
     * //@param numContainers
     *            Containers to ask for from RM
     * @return the setup ResourceRequest to be sent to RM
     */
    private ContainerRequest setupContainerAskForRM() {
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(requestPriority);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(containerMemory);
        capability.setVirtualCores(2);

        ContainerRequest request = new ContainerRequest(capability, null, null,
                pri);
        LOG.info("Requested container ask: " + request.toString());
        return request;
    }
}