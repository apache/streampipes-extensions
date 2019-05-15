package org.streampipes.processors.geo.jvm.processors.dataOperators.validator;

public class ValidaterEnums {


    public enum FilterType {
        IsEmpty(1), IsValid (2), IsSimple (3);


        private final int option;

        FilterType(int option) {
            this.option = option;
        }

        public int getOption() {
            return option;
        }
    }


    public enum ValidationTypes {
        VALID (1), INVALID (2);

        private final int option;


        ValidationTypes(int option) {
            this.option = option;
        }
    }
}
