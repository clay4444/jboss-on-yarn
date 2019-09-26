
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 * Client for JBoss Application Master submission to YARN
 * 提交到Yarn的应用程序客户端
 */
public class JBossClient {

    private static final Logger LOG = Logger.getLogger(JBossClient.class
            .getName());

    private Configuration conf;
    private YarnClient yarnClient;
    private String appName = "";
    private int amPriority = 0;    //AppMaster 优先级
    private String amQueue = "";   //AppMaster 所属队列
    private int amMemory = 1024;   //AppMaster 内存大小

    private String appJar = "";
    private final String appMasterMainClass = JBossApplicationMaster.class.getName();  //AppMaster的主类名？

    private int shellCmdPriority = 0;

    private int containerMemory = 1024;  //单个container的内存大小
    private int numContainers = 2;   //container的个数

    private String adminUser;       //用户名
    private String adminPassword;   //密码
    private String jbossAppUri;    //jar包路径

    private String log4jPropFile = "";

    boolean debugFlag = false;

    private Options opts;

    /**
     * @param args
     *            Command line arguments
     */
    public static void main(String[] args) {
        boolean result = false;
        try {
            JBossClient client = new JBossClient(); //用默认的YarnConfiguration初始化YarnClient、初始化Options，设置需要传递的参数
            LOG.info("Initializing JBossClient");
            try {
                boolean doRun = client.init(args);   //初始化全局变量，jar包、container资源、优先级、要提交到的队列等，
                if (!doRun) {  //初始化不成功，直接退出；
                    System.exit(0);
                }
            } catch (IllegalArgumentException e) {
                System.err.println(e.getLocalizedMessage());
                client.printUsage();
                System.exit(-1);
            }
            result = client.run(); //run 方法启动
        } catch (Throwable t) {
            LOG.log(Level.SEVERE, "Error running JBoss Client", t);
            System.exit(1);
        }
        if (result) {
            LOG.info("Application completed successfully");
            System.exit(0);
        }
        LOG.log(Level.SEVERE, "Application failed to complete successfully");
        System.exit(2);
    }

    /**
     * 默认就是调这个构造方法；
     */
    public JBossClient(Configuration conf) throws Exception {

        this.conf = conf;
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        opts = new Options();
        opts.addOption("appname", true,
                "Application Name. Default value - JBoss on YARN");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("queue", true,
                "RM Queue in which this application is to be submitted");
        opts.addOption("timeout", true, "Application timeout in milliseconds");
        opts.addOption("master_memory", true,
                "Amount of memory in MB to be requested to run the application master");
        opts.addOption("jar", true,
                "JAR file containing the application master");
        opts.addOption("container_memory", true,
                "Amount of memory in MB to be requested to run the shell command");
        opts.addOption("num_containers", true,
                "No. of containers on which the shell command needs to be executed");
        opts.addOption("admin_user", true,
                "User id for initial administrator user");
        opts.addOption("admin_password", true,
                "Password for initial administrator user");
        opts.addOption("log_properties", true, "log4j.properties file");
        opts.addOption("debug", false, "Dump out debug information");
        opts.addOption("help", false, "Print usage");
    }

    /**
     */
    public JBossClient() throws Exception {
        this(new YarnConfiguration());
    }

    /**
     * Helper function to print out usage
     */
    private void printUsage() {
        new HelpFormatter().printHelp("Client", opts);
    }

    /**
     * Parse command line options
     *
     * @param args
     *            Parsed command line options
     * @return Whether the init was successful to run the client
     * @throws ParseException
     */
    public boolean init(String[] args) throws ParseException {

        CommandLine cliParser = new GnuParser().parse(opts, args);

        /*
         * if (args.length == 0) { throw new
         * IllegalArgumentException("No args specified for client to initialize"
         * ); }
         */

        if (cliParser.hasOption("help")) {
            printUsage();
            return false;
        }

        if (cliParser.hasOption("debug")) {  //打开调试模式
            debugFlag = true;
        }

        appName = cliParser.getOptionValue("appname", "JBoss on YARN");
        amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        amQueue = cliParser.getOptionValue("queue", "default");
        amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory","10"));

        if (amMemory < 0) {
            throw new IllegalArgumentException(
                    "Invalid memory specified for application master, exiting."
                            + " Specified memory=" + amMemory);
        }

        if (!cliParser.hasOption("jar")) {
            throw new IllegalArgumentException(
                    "No jar file specified for application master");
        }

        appJar = cliParser.getOptionValue("jar");

        containerMemory = Integer.parseInt(cliParser.getOptionValue(
                "container_memory", "10"));
        numContainers = Integer.parseInt(cliParser.getOptionValue(
                "num_containers", "1"));
        adminUser = cliParser.getOptionValue("admin_user", "yarn");  //默认提交app的用户名和密码都是yarn
        adminPassword = cliParser.getOptionValue("admin_password", "yarn");

        if (containerMemory < 0 || numContainers < 1) {
            throw new IllegalArgumentException(
                    "Invalid no. of containers or container memory specified, exiting."
                            + " Specified containerMemory=" + containerMemory
                            + ", numContainer=" + numContainers);
        }

        log4jPropFile = cliParser.getOptionValue("log_properties", "");

        return true;
    }

    /**
     * Main run function for the client
     *
     * @return true if application completed successfully
     * @throws IOException
     * @throws YarnException
     */
    public boolean run() throws IOException, YarnException {


        /**
         * ============================================================================
         *              0.打印一些metrics信息
         * ============================================================================
         */
        LOG.info("Running Client");
        yarnClient.start();

        YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
        LOG.info("Got Cluster metric info from ASM" + ", numNodeManagers="
                + clusterMetrics.getNumNodeManagers());         //=== NodeManager的个数

        List<NodeReport> clusterNodeReports = yarnClient
                .getNodeReports(NodeState.RUNNING);       //=== 获取所有的NodeManager
        LOG.info("Got Cluster node info from ASM");
        for (NodeReport node : clusterNodeReports) {
            LOG.info("Got node report from ASM for" + ", nodeId="
                    + node.getNodeId() + ", nodeAddress" //id
                    + node.getHttpAddress() + ", nodeRackName" //address
                    + node.getRackName() + ", nodeNumContainers"  //机架名
                    + node.getNumContainers());  //当前NodeManager所有的container个数；
        }

        QueueInfo queueInfo = yarnClient.getQueueInfo(this.amQueue);  //=== 配置的队列信息
        LOG.info("Queue info" + ", queueName=" + queueInfo.getQueueName()
                + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
                + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
                + ", queueApplicationCount="
                + queueInfo.getApplications().size() //这个队列当前跑了多少个app
                + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());//有多少个子队列

        List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();//=== 所有队列的访问控制列表
        for (QueueUserACLInfo aclInfo : listAclInfo) {
            for (QueueACL userAcl : aclInfo.getUserAcls()) {
                LOG.info("User ACL Info for Queue" + ", queueName="
                        + aclInfo.getQueueName() + ", userAcl="
                        + userAcl.name());
            }
        }

        /**
         * ============================================================================
         *              1.客户端Application请求
         * ============================================================================
         */
        YarnClientApplication app = yarnClient.createApplication();

        /**
         * ============================================================================
         *              2.应答带回ApplicationId
         * ============================================================================
         */
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

        //在请求注册的响应中，ResourceManager会回应关于集群的最小最大容量，在这一点上，ApplicationMaster必须决定如何使用当前可用的资源；
        //与一些客户端请求硬限制的资源调度系统不同，Yarn允许应用程序适应当前的集群环境；
        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem); //当前集群可用的最大内存

        if (amMemory > maxMem) {
            LOG.info("AM memory specified above max threshold of cluster. Using max value."
                    + ", specified=" + amMemory + ", max=" + maxMem);
            amMemory = maxMem;
        }

        /**
         * ============================================================================
         *    3. 客户端提交Application Submission Context给ResourceManager，包含ApplicationId，用户名，队列名等，
         *    及container launch context中的资源请求，作业文件，安全令牌等；
         * ============================================================================
         */
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext(); //拿到 Application Submission 上下文信息
        ApplicationId appId = appContext.getApplicationId();  //ResourceManager 给当前app分配的 ApplicationId
        appContext.setApplicationName(appName);

        //创建CLC，CLC提供了资源需求(内存/CPU)、作业文件、安全令牌以及在节点上启动ApplicationMaster需要的其他信息；
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);  //CLC:container launch context，

        /**
         * ============================================================================
         *        3.1 为CLC设置本地化资源，这里主要是启动ApplicationMaster的Jar包；
         * ============================================================================
         */
        /**
         * 当启动一个container时，AppMaster可以指定该Container需要的所有文件，因此，这些文件都应该被本地化，一旦指定了这些文件，Yarn就会负责本地化，
         * 并且隐藏所有安全拷贝，管理以及后续的删除等引入的复杂性；
         */
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();  //需要本地化的资源

        LOG.info("Copy App Master jar from local filesystem and add to local environment");
        FileSystem fs = FileSystem.get(conf);    //也就是说路径都在hdfs上
        Path src = new Path(appJar);  //原始目录
        String pathSuffix = appName + File.separator + appId.getId()  //   appName/appId/JBossApp.jar
                + File.separator
                + JBossConstants.JBOSS_ON_YARN_APP;
        Path dst = new Path(fs.getHomeDirectory(), pathSuffix);   //  jar包要放的目标路径  /user/yarn/appName/appId/JBossApp.jar
        jbossAppUri = dst.toUri().toString();
        fs.copyFromLocalFile(false, true, src, dst);  // 拷贝jar包到目标路径
        FileStatus destStatus = fs.getFileStatus(dst);

        //LocalResource AppMaster作业文件jar包； 是需要本地化的资源
        LocalResource amJarRsrc = Records.newRecord(LocalResource.class);

        amJarRsrc.setType(LocalResourceType.FILE); //资源类型
        amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);  //资源可见性  public / private / application

        URL resourceUrl = ConverterUtils.getYarnUrlFromPath(dst);
        amJarRsrc.setResource(resourceUrl);     //资源的远程地址 一个URL
        LOG.info("resource location: " + resourceUrl.getScheme() + " ; " + resourceUrl.getHost() + " ; " + resourceUrl.getPort() + " ; " + resourceUrl.getFile());
        amJarRsrc.setTimestamp(destStatus.getModificationTime()); //时间戳，为了保证一致性，必填
        amJarRsrc.setSize(destStatus.getLen()); //资源大小
        localResources.put(JBossConstants.JBOSS_ON_YARN_APP,amJarRsrc); //<jar包名，实际jar包>

        if (!log4jPropFile.isEmpty()) {
            Path log4jSrc = new Path(log4jPropFile);
            Path log4jDst = new Path(fs.getHomeDirectory(), "log4j.props");
            fs.copyFromLocalFile(false, true, log4jSrc, log4jDst);
            FileStatus log4jFileStatus = fs.getFileStatus(log4jDst);
            LocalResource log4jRsrc = Records.newRecord(LocalResource.class);
            log4jRsrc.setType(LocalResourceType.FILE);
            log4jRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
            log4jRsrc.setResource(ConverterUtils.getYarnUrlFromURI(log4jDst
                    .toUri()));
            log4jRsrc.setTimestamp(log4jFileStatus.getModificationTime());
            log4jRsrc.setSize(log4jFileStatus.getLen());
            localResources.put("log4j.properties", log4jRsrc);
        }

        amContainer.setLocalResources(localResources);

        /**
         * ============================================================================
         *        3.1 为CLC设置环境变量
         * ============================================================================
         */
        LOG.info("Set the environment for the application master");
        Map<String, String> env = new HashMap<String, String>();

        StringBuilder classPathEnv = new StringBuilder(
                Environment.CLASSPATH.$()).append(File.pathSeparatorChar)  //pathSeparatorChar,路径分割符
                .append("./*"); //classpath 加上当前路径

        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classPathEnv.append(File.pathSeparatorChar);
            classPathEnv.append(c.trim());  //classpath 加上 yarn.application.classpath
        }
        classPathEnv.append(File.pathSeparatorChar)
                .append("./log4j.properties");

        if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classPathEnv.append(':');
            classPathEnv.append(System.getProperty("java.class.path"));
        }

        env.put("CLASSPATH", classPathEnv.toString());

        amContainer.setEnvironment(env);

        Vector<CharSequence> vargs = new Vector<CharSequence>(30);

        LOG.info("Setting up app master command");
        vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
        vargs.add("-Xmx" + amMemory + "m");
        vargs.add(appMasterMainClass);
        vargs.add("--container_memory " + String.valueOf(containerMemory));
        vargs.add("--num_containers " + String.valueOf(numContainers));
        vargs.add("--priority " + String.valueOf(shellCmdPriority));
        vargs.add("--admin_user " + adminUser);
        vargs.add("--admin_password " + adminPassword);
        vargs.add("--jar " + jbossAppUri);

        if (debugFlag) {
            vargs.add("--debug");
        }

        vargs.add("1>" + JBossConstants.JBOSS_CONTAINER_LOG_DIR
                + "/JBossApplicationMaster.stdout");
        vargs.add("2>" + JBossConstants.JBOSS_CONTAINER_LOG_DIR
                + "/JBossApplicationMaster.stderr");

        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        LOG.info("Completed setting up app master command "
                + command.toString());
        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());
        amContainer.setCommands(commands);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(amMemory);
        appContext.setResource(capability);

        appContext.setAMContainerSpec(amContainer);

        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(amPriority);
        appContext.setPriority(pri);

        appContext.setQueue(amQueue);

        LOG.info("Submitting the application to ASM");

        yarnClient.submitApplication(appContext);

        return monitorApplication(appId);
    }

    /**
     * Monitor the submitted application for completion. Kill application if
     * time expires.
     *
     * @param appId
     *            Application Id of application to be monitored
     * @return true if application completed successfully
     * @throws YarnException
     * @throws IOException
     */
    private boolean monitorApplication(ApplicationId appId)
            throws YarnException, IOException {

        while (true) {

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.finest("Thread sleep in monitoring loop interrupted");
            }

            ApplicationReport report = yarnClient.getApplicationReport(appId);

            LOG.info("Got application report from ASM for" + ", appId="
                    + appId.getId() + ", clientToAMToken="
                    + report.getClientToAMToken() + ", appDiagnostics="
                    + report.getDiagnostics() + ", appMasterHost="
                    + report.getHost() + ", appQueue=" + report.getQueue()
                    + ", appMasterRpcPort=" + report.getRpcPort()
                    + ", appStartTime=" + report.getStartTime()
                    + ", yarnAppState="
                    + report.getYarnApplicationState().toString()
                    + ", distributedFinalState="
                    + report.getFinalApplicationStatus().toString()
                    + ", appTrackingUrl=" + report.getTrackingUrl()
                    + ", appUser=" + report.getUser());

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus jbossStatus = report
                    .getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == jbossStatus) {
                    LOG.info("Application has completed successfully. Breaking monitoring loop");
                    return true;
                } else {
                    LOG.info("Application did finished unsuccessfully."
                            + " YarnState=" + state.toString()
                            + ", JBASFinalStatus=" + jbossStatus.toString()
                            + ". Breaking monitoring loop");
                    return false;
                }
            } else if (YarnApplicationState.KILLED == state
                    || YarnApplicationState.FAILED == state) {
                LOG.info("Application did not finish." + " YarnState="
                        + state.toString() + ", JBASFinalStatus="
                        + jbossStatus.toString() + ". Breaking monitoring loop");
                return false;
            }
        }
    }
}