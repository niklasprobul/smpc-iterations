import os
import shutil
import threading
import time

import numpy as np
import joblib
import jsonpickle
import pandas as pd
import yaml

from FeatureCloud.app.engine.app import AppState, app_state, Role, LogLevel, State, SMPCOperation

smpc = True

@app_state('initial', Role.BOTH)
class InitialState(AppState):

    def register(self):
        self.register_transition('local computation', Role.BOTH)
        
    def run(self) -> str or None:
        print("Initializing")
        self.store('iteration', 0)
        return 'local computation'


@app_state('local computation', Role.BOTH)
class LocalComputationState(AppState):
    
    def register(self):
        self.register_transition('global aggregation', Role.COORDINATOR)
        self.register_transition('wait for aggregation', Role.PARTICIPANT)
  
    def run(self) -> str or None:
        print("Local computation")
        print('iteration')
        print(self.load('iteration'))
        self.store('iteration', self.load('iteration') + 1)
        
        if self.is_coordinator:
            data_to_send = [1, 1, 1, 1]
            print('data_outgoing')
            print(self._app.data_outgoing)
        else:
            data_to_send = [0, 0, 0, 0]
        if smpc:
            self.configure_smpc(operation=SMPCOperation.ADD)
        self.send_data_to_coordinator(data_to_send, use_smpc=smpc)
        print("send data to coordinator")

        if self.is_coordinator:
            return 'global aggregation'
        else:
            print(f'[CLIENT] Sending computation data to coordinator', flush=True)
            return 'wait for aggregation'
                

@app_state('wait for aggregation', Role.PARTICIPANT)
class WaitForAggregationState(AppState):
    
    def register(self):
        self.register_transition('local computation', Role.PARTICIPANT)
        self.register_transition('terminal', Role.PARTICIPANT)
     
    def run(self) -> str or None:
        print("Wait for aggregation")
        data = self.await_data()
        print("Received aggregated data")

        self.send_data_to_coordinator([0], use_smpc=False)
        print("Send okay to the coordinator")
        
        check = self.await_data()
        print("Received okay from coordinator")
        
        if check != [0]:
            print('data')
            print(data)
            print('check')
            print(check)
            raise Exception('SMPC not working - Wait for aggregation')
        
        self.send_data_to_coordinator([0], use_smpc=False)
        print("Send okay second time to the coordinator")


        if self.load('iteration') < 2:
            return 'local computation'
        else:
            return 'terminal'
   
   
@app_state('global aggregation', Role.COORDINATOR)
class GlobalAggregationState(AppState):

    def register(self):
        self.register_transition('local computation', Role.COORDINATOR)
        self.register_transition('terminal', Role.COORDINATOR)

    def run(self) -> str or None:
        print("Global computation")
        print('data_incoming')
        print(self._app.data_incoming)
        print('data_outgoing')
        print(self._app.data_outgoing)

        if smpc:
            data = self.await_data(1, unwrap=True, is_json=True)
        else:
            data = self.gather_data()
        print("Received data of all clients")
        
        self.broadcast_data(data, send_to_self=False)
        print(f'[COORDINATOR] Broadcasting computation data to clients', flush=True)
        
        self.send_data_to_coordinator([2], use_smpc=False)
        print("Coordinator sends value '2' to himself")
        
        check = np.sum(self.gather_data())
        print("Received okay from all clients")
        
        print('check')
        print(check)

        if check != [2]:
            print('data')
            print(data)
            print('check')
            print(check)
            raise Exception('SMPC not working - Global computation')

        self.broadcast_data([0], send_to_self=False)
        print("Send okay to all clients")

        self.send_data_to_coordinator([2], use_smpc=False)
        print("Coordinator sends value '2' second time to himself")
        
        check = np.sum(self.gather_data())
        print("Received okay second time from all clients")

        print('data_incoming')
        print(self._app.data_incoming)
        print('data_outgoing')
        print(self._app.data_outgoing)

        if self.load('iteration') < 2:
            return 'local computation'
        else:
            return 'terminal'