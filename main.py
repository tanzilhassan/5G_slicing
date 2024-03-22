import csv
import random

class Queue:
    def __init__(self, id):
        self.id = id
        self.packets = 0  # Total packets in the queue
        self.flows = []  # Flows assigned to this queue
        self.flows_packets = []  # List to store packets (by flow ID)

class Flow:
    def __init__(self, id: int, num_packets: int, start_time: int):
        self.id = id
        self.num_packets = num_packets
        self.start_time = start_time
        self.completion_time = None
        self.inflight = 0
    
    def ack(self, num_packets, time):
        self.num_packets -= num_packets
        if self.num_packets <= 0:
            self.completion_time = time

    def send(self):
        sent_packets = min(random.randint(1, 10), self.num_packets)
        self.inflight = sent_packets
        return sent_packets

class BaseStation:
    def __init__(self, prb_data_capacity: int, num_prbs: int):
        self.queues = [Queue(id) for id in range(3)]  # Assume 3 queues
        self.time = 0
        self.prb_data_capacity = prb_data_capacity
        self.total_num_prbs = num_prbs
        self.completed_flows = []
        self.prb_allocations = []

    def add_flow(self, flow: Flow, queue_index: int):
        self.queues[queue_index].flows.append(flow)

    def fill_queues(self):
        print('\n----FILL----')
        for queue in self.queues:
            for flow in queue.flows:
                if self.time >= flow.start_time and flow.num_packets > 0 and flow.completion_time is None:
                    added_packets = flow.send()
                    queue.packets += added_packets
                    queue.flows_packets.extend([flow.id] * added_packets)
                    print(f'Flow {flow.id} added {added_packets} packets to Queue {queue.id}')

    def drain_queues(self):
        print('\n----DRAIN----')
        total_packets = sum(queue.packets for queue in self.queues)
        prbs_allocation = [0] * len(self.queues) if total_packets == 0 else [self.total_num_prbs // len(self.queues)] * len(self.queues)

        for i, queue in enumerate(self.queues):
            if queue.packets > 0:
                queue_share = queue.packets / total_packets
                prbs_for_queue = int(queue_share * self.total_num_prbs)
                prbs_allocation[i] = prbs_for_queue

                while prbs_for_queue > 0 and len(queue.flows_packets) > 0:
                    flow_id = queue.flows_packets.pop(0)
                    flow = next((f for f in queue.flows if f.id == flow_id), None)
                    if flow:
                        flow.ack(1, self.time)
                        queue.packets -= 1  # Ensure to decrement queue packets
                        prbs_for_queue -= 1
                        if flow.num_packets <= 0 and flow not in self.completed_flows:
                            self.completed_flows.append(flow)
                            print(f'Flow {flow.id} completed at time {self.time}')
                print(f'Queue {queue.id} after draining: {len(queue.flows_packets)} packets left')

        self.prb_allocations.append(prbs_allocation)
        print(f'PRB Allocation: {prbs_allocation}')

    def simulate_time_step(self):
        self.time += 1
        self.fill_queues()
        self.drain_queues()

    def simulate(self, num_steps: int):
        for _ in range(num_steps):
            print('-' * 20)
            print(f'Time Step: {self.time}')
            self.simulate_time_step()

        completion_times = [(flow.id, flow.start_time, flow.completion_time) for flow in self.completed_flows]
        return completion_times

    def write_prb_allocations_to_csv(self, filename):
        with open(filename, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['Time Step'] + [f'Queue {i} PRBs' for i in range(len(self.queues))])
            for time_step, allocation in enumerate(self.prb_allocations):
                csvwriter.writerow([time_step] + allocation)

# Set up the simulation parameters
execution_time = 1000
flows_number = 10
base_station = BaseStation(prb_data_capacity=1, num_prbs=15)

# Generate random flows and associate them with queues

random_flows = [Flow(id=i, num_packets=random.randint(5000, 100000), start_time=random.randint(0, 100)) for i in range(flows_number)]
for i, flow in enumerate(random_flows):
    queue_index = i % len(base_station.queues)
    base_station.add_flow(flow, queue_index)

# Run the simulation
completion_times = base_station.simulate(execution_time)

# After simulation, write PRB allocations to CSV
csv_filename = 'prb_allocations.csv'
base_station.write_prb_allocations_to_csv(csv_filename)

print("Flow completion times:", completion_times)