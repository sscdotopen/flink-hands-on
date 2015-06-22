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

package de.tuberlin.dima.flinkhandson.iterative;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;

import java.util.Random;


public class DiceThrowing {

  static class Participant {

    int id;
    int numWins = 0;

    public Participant() {}

    public Participant(int id) {
      this.id = id;
    }

    public Participant(int id, int numWins) {
      this.id = id;
      this.numWins = numWins;
    }

    @Override
    public String toString() {
      return "Participant{" + id + ", " + numWins + '}';
    }
  }


  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource<Participant> participants =
        env.fromElements(new Participant(1), new Participant(2), new Participant(3));

    // IMPLEMENT ME
    IterativeDataSet<Participant> iteration = null;

    // IMPLEMENT ME
    DataSet<Participant> round = null;

    // IMPLEMENT ME
    DataSet<Participant> champion = null;

    champion.print();

    env.execute();
  }

  static class SingleRound implements MapFunction<Participant, Participant> {

    int diceThrow() {
      return new Random().nextInt(5) + 1;
    }

    @Override
    public Participant map(Participant participant) throws Exception {
      boolean hasWon = diceThrow() > 3;

      if (!hasWon) {
        return participant;
      } else {
        return new Participant(participant.id, participant.numWins + 1);
      }
    }
  }




}