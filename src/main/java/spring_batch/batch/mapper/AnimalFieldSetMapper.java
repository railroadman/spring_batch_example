package spring_batch.batch.mapper;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;
import spring_batch.batch.model.Animal;

public class AnimalFieldSetMapper  implements FieldSetMapper<Animal> {

    @Override
    public Animal mapFieldSet(FieldSet fieldSet) throws BindException {
        Animal animal = new Animal();
        animal.setId(fieldSet.readString("id"));
        animal.setName(fieldSet.readString("name"));
        return animal;
    }
}