package producer.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import producer.dto.ProdutoDTO;

@Service
public class ProducerWorkerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public ProducerWorkerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void producer(ProdutoDTO dto) {

        ProducerRecord<String, String> record =
                new ProducerRecord<>(dto.route(), dto.content());

        try{

            var result = kafkaTemplate.send(record);
            System.out.println("Mensagem enviada com sucesso!");
            System.out.println("Tópico: " + result.get().getRecordMetadata().topic());
            System.out.println("Partition: " + result.get().getRecordMetadata().partition());
            System.out.println("Partition: " + result.get().getRecordMetadata().offset());

        }catch (Exception e){
            System.out.println("Erro ao enviar mensagem para o Kafka !");
            e.printStackTrace();
        }

    }

}
