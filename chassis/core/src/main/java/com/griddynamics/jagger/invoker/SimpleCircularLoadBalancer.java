/*
 * Copyright (c) 2010-2012 Grid Dynamics Consulting Services, Inc, All Rights Reserved
 * http://www.griddynamics.com
 *
 * This library is free software; you can redistribute it and/or modify it under the terms of
 * the GNU Lesser General Public License as published by the Free Software Foundation; either
 * version 2.1 of the License, or any later version.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.griddynamics.jagger.invoker;

import com.google.common.collect.AbstractIterator;
import com.griddynamics.jagger.util.Pair;

import java.util.Iterator;

/** LoadBalancer that share PairSupplier (list of pairs (query, endpoint)) with threads on kernel.
 * @author ???
 * @n
 * @par Details:
 * @details Each thread has it's own Iterator that comes from provide() method. ???
 *
 * @param <Q> query
 * @param <E> endpoint
 *
 * @ingroup Main_Distributors_group */
public class SimpleCircularLoadBalancer<Q, E> extends PairSupplierFactoryLoadBalancer<Q, E> {

    @Override
    public Iterator<Pair<Q, E>> provide() {

        return new AbstractIterator<Pair<Q,E>> () {

            private int size = pairSupplier.size();
            private int index = 0;

            @Override
            protected Pair<Q, E> computeNext() {
                if(index >= size) {
                    index = 0;
                }
                return pairSupplier.get(index++);
            }

            @Override
            public String toString() {
                return "SimpleCircularLoadBalancer iterator";
            }


        };
    }
}
