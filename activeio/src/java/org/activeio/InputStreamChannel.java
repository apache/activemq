/**
 *
 * Copyright 2004 Hiram Chirino
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.activeio;

import java.io.IOException;


/**
 * @version $Revision$
 */
public interface InputStreamChannel extends Channel {
    
    public int available() throws IOException;
    public void mark(int arg0);
    public boolean markSupported();
    public int read(byte[] arg0, int arg1, int arg2) throws IOException;
    public int read(byte[] arg0) throws IOException;
    public void reset() throws IOException;
    public long skip(long arg0) throws IOException;
    public int read() throws IOException;

}
