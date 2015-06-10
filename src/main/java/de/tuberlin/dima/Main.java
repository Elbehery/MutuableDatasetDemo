package de.tuberlin.dima;


import de.tuberlin.dima.flink.model.Person;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;

public class Main {

    static <T> void inspect(Class<T> klazz) {
        Field[] fields = klazz.getDeclaredFields();
        System.out.printf("%d fields:%n", fields.length);
        for (Field field : fields) {
            System.out.printf("%s %s %s%n",
                    Modifier.toString(field.getModifiers()),
                    field.getType(),
                    field.getName()
            );
        }
    }


    public static <B> void main(String[] args) throws Exception{

      //    Person p = new Person();
          inspect(Person.class);


    }
}
