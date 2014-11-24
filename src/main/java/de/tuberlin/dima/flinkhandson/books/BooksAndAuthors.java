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

package de.tuberlin.dima.flinkhandson.books;

import de.tuberlin.dima.flinkhandson.Config;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;

public class BooksAndAuthors {

  static int AUTHOR_ID_FIELD = 0;
  static int AUTHOR_NAME_FIELD = 1;

  static int BOOK_AUTHORID_FIELD = 0;
  static int BOOK_YEAR_FIELD = 1;
  static int BOOK_NAME_FIELD = 2;

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSet<Tuple2<Integer, String>> authors =
        env.readCsvFile(Config.pathTo("authors.tsv"))
           .fieldDelimiter('\t')
           .types(Integer.class, String.class);

    DataSet<Tuple3<Integer, Short, String>> books =
        env.readCsvFile(Config.pathTo("books.tsv"))
           .fieldDelimiter('\t')
           .types(Integer.class, Short.class, String.class);

    // IMPLEMENT THIS STEP
    DataSet<Tuple2<String, String>> bookAndAuthor = null;

    bookAndAuthor.writeAsCsv(Config.outputPathTo("bookAuthorJoin"), FileSystem.WriteMode.OVERWRITE);

    env.execute();
  }
}
