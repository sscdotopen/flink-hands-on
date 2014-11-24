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
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

public class TraceOfSum {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    /*
         1 1 3       1 0 0
      A  1 2 0    B  0 1 3
         0 1 1       2 0 5
    */

    DataSource<Cell> matrixA =
        env.fromElements(new Cell(0, 0, 1), new Cell(0, 1, 1), new Cell(0, 2, 3), new Cell(1, 0, 1), new Cell(1, 1, 2),
            new Cell(2, 1, 1), new Cell(2, 2, 1));

    DataSource<Cell> matrixB =
        env.fromElements(new Cell(0, 0, 1), new Cell(1, 1, 1), new Cell(1, 2, 3), new Cell(2, 0, 2), new Cell(2, 2, 5));


    DataSet<Cell> diagonalA = matrixA.filter(new DiagonalOnly());
    DataSet<Cell> diagonalB = matrixB.filter(new DiagonalOnly());


    DataSet<Cell> sumOfDiagonals =
        diagonalA.join(diagonalB).where("i", "j").equalTo("i", "j")
                 .with(new AddCells());

    DataSet<Double> trace = sumOfDiagonals.reduceGroup(new SumDiagonal());

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

  static class AddCells implements JoinFunction<Cell, Cell, Cell> {
    @Override
    public Cell join(Cell cellA, Cell cellB) throws Exception {
      return new Cell(cellA.i, cellA.j, cellA.value + cellB.value);
    }
  }

}
