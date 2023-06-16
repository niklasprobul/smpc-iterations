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
        self.store('iteration', self.load('iteration') + 1)
        data_to_send = [1, 2, 3, 4]
        if smpc:
            self.configure_smpc(operation=SMPCOperation.ADD)
        self.send_data_to_coordinator(data_to_send, use_smpc=smpc)

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
        if smpc:
            data = self.await_data(1, unwrap=True, is_json=True)
        else:
            data = self.gather_data()
        print("Received data of all clients")
        print('data')
        print(data)

        self.broadcast_data(data, send_to_self=False)
        print(f'[COORDINATOR] Broadcasting computation data to clients', flush=True)

        if self.load('iteration') < 2:
            return 'local computation'
        else:
            return 'terminal'