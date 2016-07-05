/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.flink.runtime.util;

import java.util.Iterator;

/**
 * Superclass for {@link ReusingMutableToRegularIteratorWrapper} and
 * {@link NonReusingMutableToRegularIteratorWrapper} that wraps the
 * hasNextWrapped() method.
 *
 */
abstract class IteratorWrapper<T> implements Iterator<T>, Iterable<T> {
	private boolean done = false;

	/**
	 * Wraps hasNextWrapped() so that if hasNextWrapped() has previously returned
	 * false, this function returns false. This is because hasNextWrapped() will
	 * hang if it is executed after it has already returned false once.
	 * @return
     */
	public boolean hasNext() {
		if (done) {
			return false;
		} else {
			if (!hasNextWrapped()) {
				done = true;
				return false;
			} else {
				return true;
			}
		}
	}

	abstract boolean hasNextWrapped();
}
