#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from streampipes.core import EventProcessor


class Greeter(EventProcessor):
    """
    Greeter processor

    This processor uses a user-defined greeting text and appends it to the event stream.
    """
    greeting = None

    def on_invocation(self):
        # extract greeting text from static property
        self.greeting = self.static_properties.get('greeting')

    def on_event(self, event):
        # dict key must match the append property specified in the java part
        event['greeting'] = self.greeting
        return event

    def on_detach(self):
        pass
