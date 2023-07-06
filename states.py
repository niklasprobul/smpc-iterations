import time
import pickle

from FeatureCloud.app.engine.app import AppState, app_state, Role, SMPCOperation

smpc = True
sleep = False
use_ack = False
n_iterations = 100


@app_state('initial', Role.BOTH)
class InitialState(AppState):

    def register(self):
        self.register_transition('local computation', Role.BOTH)

    def run(self) -> str or None:
        self.store('iteration', 0)
        return 'local computation'


@app_state('local computation', Role.BOTH)
class LocalComputationState(AppState):

    def register(self):
        self.register_transition('global aggregation', Role.COORDINATOR)
        self.register_transition('wait for aggregation', Role.PARTICIPANT)
        self.register_transition('terminal', Role.BOTH)

    def run(self) -> str or None:
        print('Local computation')
        iteration = self.load('iteration')
        print(f'iteration: {iteration}')

        self.store('iteration', iteration + 1)

        if len(self._app.data_outgoing) > 0 and False:
            #print(f'last outgoing data: {self._app.data_outgoing[0]}')
            # outgoing = pickle.loads(self._app.data_outgoing[0][0])
            outgoing = pickle.loads(self._app.data_outgoing[0][0])
            print(f'data_outgoing: {outgoing}')
            print('####\nData outgoing not empty\n####')
            return 'terminal'

        # returns an array with len = number of clients, 1 for the client id of the executor, 0 otherwise
        # should result in a n array with all 1s after aggregation
        #data_to_send = [1 if self.id == _ else 0 for _ in self.clients]
        data_to_send = 1
        if smpc:
            self.configure_smpc(operation=SMPCOperation.ADD)
        self.send_data_to_coordinator(data_to_send, use_smpc=smpc)
        print(f'Sending {data_to_send}  to coordinator')

        if self.is_coordinator:
            return 'global aggregation'
        else:
            return 'wait for aggregation'


@app_state('wait for aggregation', Role.PARTICIPANT)
class WaitForAggregationState(AppState):

    def register(self):
        self.register_transition('local computation', Role.PARTICIPANT)
        self.register_transition('terminal', Role.PARTICIPANT)

    def run(self) -> str or None:
        print('Wait for aggregation')
        data = self.await_data()
        print(f'Received aggregated data: {data}')

        if data != [1 for _ in self.clients]:
            print('####\nData not aggregated correctly\n####')
            return 'terminal'

        if use_ack:
            self.send_data_to_coordinator(0, use_smpc=False)
            print('Send okay to the coordinator')

            okay = self.await_data()
            print(f'Received okay from coordinator: {okay}')

        if self.load('iteration') < n_iterations:
            return 'local computation'
        else:
            return 'terminal'


@app_state('global aggregation', Role.COORDINATOR)
class GlobalAggregationState(AppState):

    def register(self):
        self.register_transition('local computation', Role.COORDINATOR)
        self.register_transition('terminal', Role.COORDINATOR)

    def run(self) -> str or None:
        if smpc:
            print(f'aggregating data')
            data = self.aggregate_data(SMPCOperation.ADD, use_smpc=smpc)
            #data = self.await_data(1, unwrap=True, is_json=True)
        else:
            data = self.gather_data()
        print(f'Received data of all clients: {data}')

    
        self.broadcast_data(data, send_to_self=True)
        print('Broadcasting computation data to clients')
        
        if use_ack:
            self.send_data_to_coordinator(0, use_smpc=False)
            print('Coordinator sends okay')

            okay = self.gather_data()
            print(f'Receive okay from all clients: {okay}')

            self.broadcast_data(0, send_to_self=False)
            print('Send okay to all clients')

        if sleep:
            time.sleep(100)

        if self.load('iteration') < n_iterations:
            return 'local computation'
        else:
            return 'terminal'
