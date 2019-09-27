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
    private NMCallbackHandler containerListener; //NodeManager的回调

    private ApplicationAttemptId appAttemptID;

    private String appMasterHostname = "";
    private int appMasterRpcPort = 0;
    private String appMasterTrackingUrl = "";

    private int numTotalContainers = 2;  //container 个数
    private int containerMemory = 1024;  //单个container的内存大小
    private int requestPriority;       //优先级

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

    private List<Thread> launchThreads = new ArrayList<Thread>();  //多个线程，每个线程负责启动一个container

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
            result = appMaster.run();  //run方法，主要的执行方法；
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

        /**
         * 构建 ResourceManager 通信客户端，包含一个 rm 事件(container分配/container完成)回调器
         */
        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        resourceManager = AMRMClientAsync.createAMRMClientAsync(1000,
                allocListener);
        resourceManager.init(conf);
        resourceManager.start();

        /**
         * 构建 NodeManager 通信客户端，包含一个 nm 事件(主要是container的状态变更)回调器
         */
        containerListener = new NMCallbackHandler();
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        //使用构建好的ResourceManager客户端注册ApplicationMaster，成功后启动和ResourceManager的通信线程
        RegisterApplicationMasterResponse response = resourceManager
                .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
                        appMasterTrackingUrl);

        //查看最大可用内存
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

        /**
         * 按照设置的numTotalContainers，请求container，然后触发ResourceManager的回调；
         * 当收到分配的container的回调事件时，使用线程池构建CLC，启动container，
         */
        for (int i = 0; i < numTotalContainers; ++i) {
            ContainerRequest containerAsk = setupContainerAskForRM();
            resourceManager.addContainerRequest(containerAsk);
        }
        numRequestedContainers.set(numTotalContainers); //要请求的container一开始 = 指定的container总数

        /**
         * 循环等待结束，最终的状态可能是：
         * 0.所有请求的container都成功结束 (最好的情况，也应该是大部分的时候出现的情况)
         * 1.所有请求的container都已经结束，（ 不保证所有container都正常结束，可能有的container是无法恢复的失败 ）
         * 2.rm 因为不同步或者其他原因，希望 shutdown 此ApplicationMaster
         * 2.ApplicationMaster 本身程序发生异常 / 和rm 通信异常
         */
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
                launchThread.join(10000);       //等待所有启动container的线程执行完毕；
            } catch (InterruptedException e) {
                LOG.info("Exception thrown in thread join: " + e.getMessage());
                e.printStackTrace();
            }
        }

        LOG.info("Application completed. Stopping running containers");
        nmClientAsync.stop(); //停止NodeManager 客户端

        LOG.info("Application completed. Signalling finish to RM");

        FinalApplicationStatus appStatus;  //AppMaster最终状态
        String appMessage = null;
        success = true;
        if (numFailedContainers.get() == 0
                && numCompletedContainers.get() == numTotalContainers) {
            appStatus = FinalApplicationStatus.SUCCEEDED; //此App成功
        } else {
            appStatus = FinalApplicationStatus.FAILED;     //此App失败
            appMessage = "Diagnostics." + ", total=" + numTotalContainers //打印诊断信息；
                    + ", completed=" + numCompletedContainers.get()
                    + ", allocated=" + numAllocatedContainers.get()
                    + ", failed=" + numFailedContainers.get();
            success = false; //标志位
        }
        try {
            resourceManager.unregisterApplicationMaster(appStatus, appMessage,null);//从 RM 取消此 AppMaster
        } catch (YarnException ex) {
            LOG.log(Level.SEVERE, "Failed to unregister application", ex);
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "Failed to unregister application", e);
        }

        done = true;
        resourceManager.stop(); //停止 ResourceManager 客户端
    }

    //和 ResourceManager 通信的回调
    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

        /**
         * 当ResourceManager回复一个心跳内容，且心跳内容包含完成的containers的信息时，被调用；
         * 如果当前心跳既包含完成的container信息，又包含分配的container信息时，此方法在 onContainersAllocated() 方法之前调用；
         * @param completedContainers
         */
        @SuppressWarnings("unchecked")
        public void onContainersCompleted(
                List<ContainerStatus> completedContainers) {
            LOG.info("Got response from RM for container ask, completedCnt="
                    + completedContainers.size());
            for (ContainerStatus containerStatus : completedContainers) {
                LOG.info("Got container status for containerID="
                        + containerStatus.getContainerId() + ", state="
                        + containerStatus.getState() + ", exitStatus="          //退出状态
                        + containerStatus.getExitStatus() + ", diagnostics="   //诊断信息
                        + containerStatus.getDiagnostics());

                assert (containerStatus.getState() == ContainerState.COMPLETE); // ?

                int exitStatus = containerStatus.getExitStatus(); //退出码
                if (0 != exitStatus) {  //非正常结束
                    if (ContainerExitStatus.ABORTED != exitStatus) {  //真正的失败，不用继续考虑恢复
                        numCompletedContainers.incrementAndGet(); //已完成+1
                        numFailedContainers.incrementAndGet();    //失败的+1
                    } else {                               //container被框架kill了  ABORTED状态可能是被主动释放的 / 节点故障，这种需要继续申请container来恢复
                        numAllocatedContainers.decrementAndGet(); //已分配的-1，需要重新分配
                        numRequestedContainers.decrementAndGet(); //已请求的-1，需要重新请求
                    }
                } else {  //程序正常结束，
                    numCompletedContainers.incrementAndGet(); //已完成的containers容器+1
                    LOG.info("Container completed successfully."
                            + ", containerId="
                            + containerStatus.getContainerId());
                }
            }

            int askCount = numTotalContainers - numRequestedContainers.get();  //总共需要的 - 已经请求的 = 还要再请求的
            numRequestedContainers.addAndGet(askCount);

            if (askCount > 0) {
                for (int i = 0; i < askCount; ++i) {
                    ContainerRequest containerAsk = setupContainerAskForRM();
                    resourceManager.addContainerRequest(containerAsk);      //继续请求
                }
            }

            if (numCompletedContainers.get() == numTotalContainers) {  //要申请的container任务已全部完成；
                done = true;   //程序结束
            }
        }

        /**
         * 收到了ResourceManager 分配的 containers
         * 然后就在每个container上启动应用；
         */
        public void onContainersAllocated(List<Container> allocatedContainers) {
            LOG.info("Got response from RM for container ask, allocatedCnt="
                    + allocatedContainers.size());
            numAllocatedContainers.addAndGet(allocatedContainers.size());   //numAllocatedContainers(分配的) + size
            for (Container allocatedContainer : allocatedContainers) {
                LOG.info("Launching shell command on a new container." //在每个container上启动应用；
                        + ", containerId=" + allocatedContainer.getId()
                        + ", containerNode="
                        + allocatedContainer.getNodeId().getHost() + ":"  //host:port
                        + allocatedContainer.getNodeId().getPort()
                        + ", containerNodeURI="
                        + allocatedContainer.getNodeHttpAddress()  //URL
                        + ", containerResourceMemory"
                        + allocatedContainer.getResource().getMemory());  //内存

                //子线程负责各个container的启动
                LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer, containerListener);
                Thread launchThread = new Thread(runnableLaunchContainer);

                launchThreads.add(launchThread);
                launchThread.start();
            }
        }

        //当ResourceManager因为不同步等原因希望shutdown Application Master时调用；
        public void onShutdownRequest() {
            done = true;
        }

        //当NodeManager的状态(健康状态 / 是否可用)发生更新时回调，
        public void onNodesUpdated(List<NodeReport> updatedNodes) {
        }

        //进度，已经完成的container / 总共需要申请的container，
        public float getProgress() {
            float progress = (float) numCompletedContainers.get()
                    / numTotalContainers;
            return progress;
        }

        //发生异常时回调，异常可能是由于和ResourceManager的通信异常 / 程序自身的异常，此时建议调用 resourceManager.stop(); 方法
        public void onError(Throwable e) {
            done = true;
            resourceManager.stop();
        }
    }

    //实现NMClientAsync(和NodeManager进行异步通信)的 CallbackHandler
    //相当于和NodeManager通信后的 异步获取结果的 回调方法；
    private class NMCallbackHandler implements NMClientAsync.CallbackHandler {

        //相当于一个NodeManager上有多个 container ?
        private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();

        public void addContainer(ContainerId containerId, Container container) {
            containers.putIfAbsent(containerId, container);
            LOG.info("Callback container id : " + containerId.toString());

            if (containers.size() == 1) {
                domainController = container.getNodeId().getHost();
            }
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Succeeded to stop Container " + containerId);
            }
            containers.remove(containerId);
        }

        @Override
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
                nmClientAsync.getContainerStatusAsync(containerId,  //container启动后，通过NodeManager客户端监听container的状态；
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
     * 通过执行shell命令启动container
     * Thread to connect to the {@link ContainerManagementProtocol} and launch
     * the container that will execute the shell command.
     */
    private class LaunchContainerRunnable implements Runnable {

        Container container;

        NMCallbackHandler containerListener;

        /**
         * @param lcontainer
         *            Allocated container  被分配的container
         * @param containerListener
         *            Callback handler of the container  被分配container的监听器
         */
        public LaunchContainerRunnable(Container lcontainer,
                                       NMCallbackHandler containerListener) {
            this.container = lcontainer;
            this.containerListener = containerListener;
        }

        /**
         * Connects to CM(ContainerManagementProtocol), sets up container launch context for shell command
         * and eventually dispatches the container start request to the CM.
         */
        public void run() {

            String containerId = container.getId().toString();

            LOG.info("Setting up container launch container for containerid="
                    + container.getId());

            //1.构建CLC!
            ContainerLaunchContext ctx = Records
                    .newRecord(ContainerLaunchContext.class);

            ctx.setEnvironment(shellEnv); //1.1 为 CLC 构建执行环境


            //1.2 为 CLC 设置需要本地化的资源(jboss本身的jar和要部署到jboss中运行的jar包，要部署到jboss中运行的jar已经被拷贝到了home目录 )
            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

            String applicationId = container.getId().getApplicationAttemptId()
                    .getApplicationId().toString();
            try {
                FileSystem fs = FileSystem.get(conf);

                LocalResource jbossDist = Records
                        .newRecord(LocalResource.class);
                jbossDist.setType(LocalResourceType.ARCHIVE); //压缩文件
                jbossDist.setVisibility(LocalResourceVisibility.APPLICATION); //应用级别

                Path jbossDistPath = new Path(new URI(
                        JBossConstants.JBOSS_DIST_PATH));  //1. jBoss本身jar包；hdfs://yarn1.apps.hdp:9000/apps/jboss/dist/jboss-as-7.1.1.Final.tar.gz
                jbossDist.setResource(ConverterUtils
                        .getYarnUrlFromPath(jbossDistPath));

                jbossDist.setTimestamp(fs.getFileStatus(jbossDistPath)   //时间戳
                        .getModificationTime());
                jbossDist.setSize(fs.getFileStatus(jbossDistPath).getLen());  //大小
                localResources.put(JBossConstants.JBOSS_SYMLINK, jbossDist);  //放进 localResources 这个map

                LocalResource jbossConf = Records
                        .newRecord(LocalResource.class);
                jbossConf.setType(LocalResourceType.FILE);      //2. 要放进jboss容器中部署的应用jar
                jbossConf.setVisibility(LocalResourceVisibility.APPLICATION);

                Path jbossConfPath = new Path(new URI(appJar));  //拷贝到home目录的应用jar，  /user/yarn/appName/appId/JBossApp.jar
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

            //1.1 为 CLC 构建执行命令
            List<String> commands = new ArrayList<String>();

            String host = container.getNodeId().getHost(); //host地址；

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

            //2.对此container设置监听
            containerListener.addContainer(container.getId(), container);

            //3.使用构建好的CLC启动该container
            nmClientAsync.startContainerAsync(container, ctx);
        }
    }

    /**
     * 构建向ResourceManager发送container请求的Request
     * Setup the request that will be sent to the RM for the container ask.
     *
     * //@param numContainers
     *            Containers to ask for from RM
     * @return the setup ResourceRequest to be sent to RM
     */
    private ContainerRequest setupContainerAskForRM() {
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(requestPriority); //优先级

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(containerMemory); //内存
        capability.setVirtualCores(2);       //CPU核数

        ContainerRequest request = new ContainerRequest(capability, null, null,pri);
        LOG.info("Requested container ask: " + request.toString());
        return request;
    }
}