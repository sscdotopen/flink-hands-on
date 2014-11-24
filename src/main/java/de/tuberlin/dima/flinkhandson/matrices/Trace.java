/**
 * Flink Hands-on
 * Copyright (C) 2014  Sebastian Schelter
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.flinkhandson.matrices;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

public class Trace {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    /*
      [ 0 1 3;
        1 2 0;
        0 1 1]
    */

    DataSource<Cell> matrix =
        env.fromElements(new Cell(0, 1, 1), new Cell(0, 2, 3), new Cell(1, 0, 1), new Cell(1, 1, 2),
                         new Cell(2, 1, 1), new Cell(2, 2, 1));


    DataSet<Double> trace =
        matrix.filter(new DiagonalOnly())
              .reduceGroup(new SumDiagonal());

    trace.print();

    env.execute();

  }

  static class DiagonalOnly implements FilterFunction<Cell> {

    @Override
    public boolean filter(Cell cell) throws Exception {
      return cell.i == cell.j;
    }
  }

  static class SumDiagonal implements GroupReduceFunction<Cell, Double> {
    @Override
    public void reduce(Iterable<Cell> cells, Collector<Double> collector) throws Exception {
      double trace = 0;
      for (Cell cell: cells) {
        trace += cell.value;
      }
      collector.collect(trace);
    }
  }
}
