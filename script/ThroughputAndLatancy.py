import matplotlib.pyplot as plt
import json
import numpy as np
from itertools import zip_longest

class container_metrics_class:
    def __init__(self, containerId):
        self.containerId = containerId
        self.service_rate_list = []
        self.utilization_list = []
        self.average_latency_list = []

    def add_metrics(self, service_rate, utilization, average_latency):
        self.service_rate_list.append(service_rate)
        self.utilization_list.append(utilization)
        self.average_latency_list.append(average_latency)

    def get_service_rate_list(self):
        return self.service_rate_list

    def get_utilization_list(self):
        return self.utilization_list

    def get_average_latency_list(self):
        return self.average_latency_list


if __name__ == "__main__":
    fp = open("/home/myc/workspace/SSE-anaysis/data/source/metrics.json")
    container_map = {}

    # service_rate_list = []
    # utilization_list = []
    # average_latency_list = []

    text = fp.readline()
    while text:
        json_data = json.loads (text)
        if "org.apache.samza.container.SamzaContainerMetrics" in json_data["metrics"][1] :
            container_name = json_data["header"][1]["exec-env-container-id"]
            service_rate = json_data["metrics"][1]["org.apache.samza.container.SamzaContainerMetrics"][1]["service-rate"]
            average_utilization = json_data["metrics"][1]["org.apache.samza.container.SamzaContainerMetrics"][1]["average-utilization"]
            average_latency = json_data["metrics"][1]["org.apache.samza.container.SamzaContainerMetrics"][1]["average-latency"][1]
            if container_name not in container_map:
                container_metrics = container_metrics_class(container_name)
                container_map[container_name] = container_metrics
            container_map[container_name].add_metrics(service_rate, average_utilization, average_latency)
        text = fp.readline()

    print(container_map)
    container_metrics = container_map["container_1561015851560_0049_01_000002"]

    app_service_rate_list = {}
    app_average_latency_list = {}
    service_rate_list = {}
    average_latency_list = {}
    for key in container_map:
        stage_attempt = key[:-7]
        if stage_attempt not in app_service_rate_list:
            app_service_rate_list[stage_attempt] = []
            app_average_latency_list[stage_attempt] = []
        app_service_rate_list[stage_attempt].append(container_map[key].service_rate_list)
        app_average_latency_list[stage_attempt].append(container_map[key].average_latency_list)


    for key in app_service_rate_list:
        service_rate_list = [sum(x) for x in zip_longest(*app_service_rate_list[key], fillvalue=0)]
        average_latency_list = [sum(x)/len(x) for x in zip_longest(*app_average_latency_list[key], fillvalue=0)]

        plt.suptitle(key)

        plt.subplot(2, 1, 1)
        plt.plot(service_rate_list)
        plt.ylabel('Service Rate(tuple/s)')

        plt.subplot(2, 1, 2)
        plt.plot(average_latency_list)
        plt.xlabel('time (s)')
        plt.ylabel('Avg latency(ms)')

        plt.show()
