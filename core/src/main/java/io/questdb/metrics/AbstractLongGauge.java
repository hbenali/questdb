/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.metrics;

import io.questdb.std.str.BorrowableUtf8Sink;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

abstract class AbstractLongGauge implements LongGauge {
    private final CharSequence name;

    AbstractLongGauge(CharSequence name) {
        this.name = name;
    }

    @Override
    public CharSequence getName() {
        return name;
    }

    @Override
    public void scrapeIntoPrometheus(@NotNull BorrowableUtf8Sink sink) {
        appendType(sink);
        appendMetricName(sink);
        PrometheusFormatUtils.appendSampleLineSuffix(sink, getValue());
        PrometheusFormatUtils.appendNewLine(sink);
    }

    private void appendMetricName(CharSink<?> sink) {
        sink.putAscii(PrometheusFormatUtils.METRIC_NAME_PREFIX);
        sink.put(name);
    }

    private void appendType(CharSink<?> sink) {
        sink.putAscii(PrometheusFormatUtils.TYPE_PREFIX);
        sink.put(name);
        sink.putAscii(" gauge\n");
    }
}