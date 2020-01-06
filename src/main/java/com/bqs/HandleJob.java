package com.bqs;

import com.cloudera.api.swagger.*;
import com.cloudera.api.swagger.client.ApiClient;
import com.cloudera.api.swagger.client.Configuration;
import com.cloudera.api.swagger.model.*;
import javafx.util.Pair;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;

public class HandleJob implements Job {

    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private HostsResourceApi hostsResourceApi = null;
    private ClustersResourceApi clustersResourceApi = null;
    private ServicesResourceApi servicesResourceApi = null;
    private RolesResourceApi rolesResourceApi = null;
    private RoleCommandsResourceApi roleCommandsResourceApi = null;

    public HandleJob() {
        clouderaClient();
    }

    public Properties readProperties() {
        Properties prop = new Properties();
        InputStream inputStream = Object.class.getResourceAsStream("/cdhAgent.properties");
        InputStreamReader inputStreamReader = null;
        try {
            inputStreamReader = new InputStreamReader(inputStream, "GBK");
            prop.load(inputStreamReader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop;
    }

    public void clouderaClient() {
        Properties prop = readProperties();
        ApiClient cmClient = Configuration.getDefaultApiClient();

        // Configure HTTP basic authorization: basic
        cmClient.setBasePath(prop.getProperty("cmUrl"));
        cmClient.setUsername(prop.getProperty("username"));
        cmClient.setPassword(prop.getProperty("password"));

        this.hostsResourceApi = new HostsResourceApi(cmClient);
        this.clustersResourceApi = new ClustersResourceApi(cmClient);
        this.servicesResourceApi = new ServicesResourceApi(cmClient);
        this.rolesResourceApi = new RolesResourceApi(cmClient);
        this.roleCommandsResourceApi = new RoleCommandsResourceApi(cmClient);
    }

    public void handle() throws Exception {
        Date now = new Date();
        String currentDate = sdf.format(now);
        System.out.println("现在时间是："+currentDate+"：开始执行任务");

        Map<String, ApiHost> hostMap = new HashMap<String, ApiHost>();
        ApiHostList apiHosts = hostsResourceApi.readHosts("summary");
        for (ApiHost apiHost : apiHosts.getItems()) {
            hostMap.put(apiHost.getHostId(), apiHost);
        }

        ApiClusterList apiClusters = clustersResourceApi.readClusters("base", "summary");
        for (ApiCluster apiCluster : apiClusters.getItems()) {
            ApiServiceList apiServices = servicesResourceApi.readServices(apiCluster.getName(), "summary");
            for (ApiService apiService : apiServices.getItems()) {
                if (!"STARTED".equals(apiService.getServiceState().toString())) {
                    continue;
                }
                ApiRoleList apiRoleList = rolesResourceApi.readRoles(apiCluster.getName(), apiService.getName(), "", "summary");
                ApiRoleNameList body = new ApiRoleNameList();
                for (ApiRole apiRole : apiRoleList.getItems()) {
                    if (!"STARTED".equals(apiRole.getRoleState().toString())) {
                        continue;
                    }
                    List<ApiHealthCheck> apiHealthChecks = apiRole.getHealthChecks();
                    for (ApiHealthCheck apiHealthCheck : apiHealthChecks) {
                        if (apiHealthCheck.getName().contains("SCM_HEALTH") && "BAD".equals(apiHealthCheck.getSummary().toString())) {
                            String apiInfo = hostMap.get(apiRole.getHostRef().getHostId()).getIpAddress() + ":" + apiRole.getType();

                            Integer count = Frequency.frequencyMap.get(apiInfo);
                            if (count == null) count = 0;
                            if (count != 0) {
                                System.out.println(apiInfo + " restart failed, because it has been restarted");
                            } else {
                                System.out.println(apiInfo + " is restarting");
                                Frequency.frequencyMap.put(apiInfo, count + 1);
                                Frequency.intervalMap.put(apiInfo, now.getTime());
                                Frequency.queue.add(new Pair<>(now.getTime(), apiInfo));
                                body.addItemsItem(apiRole.getName());
                            }
                        }
                    }
                }
                roleCommandsResourceApi.restartCommand(apiCluster.getName(), apiService.getName(), body);
            }
        }
        while(!Frequency.queue.isEmpty()) {
            Pair<Long, String> psl = Frequency.queue.peek();
            //10 分钟内重启一次
            if (now.getTime() - psl.getKey() >= 600000) {
                if (now.getTime() - Frequency.intervalMap.get(psl.getValue()) >= 600000) {
                    System.out.println("More than 10 minutes from the last restart, remove the "+ psl.getValue() + " to release the limit");
                    Frequency.frequencyMap.remove(psl.getValue());
                    Frequency.intervalMap.remove(psl.getValue());
                }
                Frequency.queue.poll();
                continue;
            }
            break;
        }
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        try {
            handle();
        } catch (Exception e) {
            e.printStackTrace();
            throw new JobExecutionException("handleJob failed");
        }
    }
}
