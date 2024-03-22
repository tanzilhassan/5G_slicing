import csv
import random

class Queue:
    def __init__(self, id):
        self.id = id
        self.packets = 0  # Total packets in the queue
        self.flows = []  # Flows assigned to this queue
        self.flows_packets = []  # List to store packets (by flow ID)
        self.queue_size = 20

class Flow:
    def __init__(self, id: int, num_packets: int, start_time: int):
        self.id = id
        self.num_packets = num_packets
        self.start_time = start_time
        self.completion_time = None
        self.inflight = 0
    
    def ack(self, num_packets, time):
        self.num_packets -= num_packets
        if self.num_packets <= 0 and self.completion_time==None:
            self.completion_time = time
            print(f'Flow {self.id} completed at time {time}')
            return True
        return False

    def send(self):
        self.inflight = min(self.inflight + random.randint(1, 10), self.num_packets)
        return self.inflight
    
    def drop_pkts(self, inflight):
        self.inflight = inflight

class BaseStation:
    def __init__(self, prb_data_capacity: int, num_prbs: int):
        self.queues = [Queue(id) for id in range(3)]  # Assume 3 queues
        self.time = 0
        self.prb_data_capacity = prb_data_capacity
        self.total_num_prbs = num_prbs
        self.completed_flows = []
        self.prb_allocations = []
        self.prb_used = []

    def add_flow(self, flow: Flow, queue_index: int):
        self.queues[queue_index].flows.append(flow)

    def fill_queues(self):
        print('\n----FILL----')
        for queue in self.queues:
            for flow in queue.flows:
                if self.time >= flow.start_time and flow.num_packets > 0 and flow.completion_time is None:
                    added_packets = flow.send()  # Attempt to send packets
                    
                    # Calculate packets that can be actually added to the queue without exceeding its limit
                    packets_to_add = min(added_packets, queue.queue_size - queue.packets)
                    
                    # Update queue state
                    queue.packets += packets_to_add
                    queue.flows_packets.extend([flow.id] * packets_to_add)
                    
                    # Calculate the number of packets that couldn't be added due to queue limit
                    packets_dropped = added_packets - packets_to_add
                    
                    # Update the flow's state based on dropped packets (if any)
                    flow.drop_pkts(packets_dropped)
                    
                    print(f'Flow {flow.id} added {packets_to_add} packets to Queue {queue.id}. Dropped {packets_dropped} packets.')
            print('Queue'+str(queue.id), queue.flows_packets)


    def slicing(self):    
        # Solve how many prbs to assign to each queue
        total_packets = sum(queue.packets for queue in self.queues)

        prbs_allocation = [0] * len(self.queues)
        if total_packets > 0:
            for i, queue in enumerate(self.queues):
                proportion = queue.packets / total_packets
                prbs_allocation[i] = int(proportion * self.total_num_prbs)

        # prbs_allocation = [0] * len(self.queues) if total_packets == 0 else [self.total_num_prbs // len(self.queues)] * len(self.queues)
        self.prb_allocations.append(prbs_allocation)

        return prbs_allocation
    
    def scheduling(self, prbs_allocation, prbs_allocation_used):

        for i, queue in enumerate(self.queues):
            if queue.packets > 0:

                while prbs_allocation_used[i] < prbs_allocation[i] and len(queue.flows_packets) > 0:
                    flow_id = queue.flows_packets.pop(0)
                    flow = next((f for f in queue.flows if f.id == flow_id), None)
                    if flow:
                        completed = flow.ack(1, self.time) # confirm it was digested
                        queue.packets -= 1  # Ensure to decrement queue packets
                        prbs_allocation_used[i] += 1
                        if completed:
                            self.completed_flows.append(flow)
                            completed = False
                print(f'Queue:{queue.id}, length:{len(queue.flows_packets)}')
        self.prb_used.append(prbs_allocation_used)
        

        print(f'PRB allocated: {prbs_allocation}, PRB used: {prbs_allocation_used}')



    def drain_queues(self):
        print('\n----DRAIN----')

        prbs_allocation = self.slicing()
        prbs_allocation_used = [0] * len(self.queues)

        self.scheduling(prbs_allocation, prbs_allocation_used)



    def simulate_time_step(self):
        self.time += 1
        self.fill_queues()
        self.drain_queues()

    def simulate(self, num_steps: int):
        for _ in range(num_steps):
            print('-' * 20)
            print(f'Time Step: {self.time}')

            if len(self.completed_flows) == len(random_flows):
                break

            self.simulate_time_step()

            completion_times = [(flow.id, flow.start_time, flow.completion_time) for flow in self.completed_flows]
            print('completed:', completion_times)

        return completion_times

    def write_prb_allocations_to_csv(self, filename):
        with open(filename, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['Time Step'] + [f'Queue {i} PRBs' for i in range(len(self.queues))])
            for time_step, allocation in enumerate(self.prb_used):
                csvwriter.writerow([time_step] + allocation)

# Set up the simulation parameters
execution_time = 1000
flows_number = 100
base_station = BaseStation(prb_data_capacity=1, num_prbs=25)

# Generate random flows and associate them with queues

random_flows = [Flow(id=i, num_packets=random.randint(10, 20), start_time=random.randint(0, 10)) for i in range(flows_number)]
# random_flows = [Flow(id=i, num_packets=10, start_time=0) for i in range(flows_number)]
for i, flow in enumerate(random_flows):
    queue_index = i % len(base_station.queues)
    print('flow:'+str(i), 'on queue:'+str(queue_index))
    base_station.add_flow(flow, queue_index)

# Run the simulation
completion_times = base_station.simulate(execution_time)

# After simulation, write PRB allocations to CSV
csv_filename = 'prb_allocations.csv'
base_station.write_prb_allocations_to_csv(csv_filename)

print("Flow completion times:", completion_times)