package consumer.service;

import consumer.dto.ProdutoDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ConsumerWorkerService {

    private final ConcurrentKafkaListenerContainerFactory factory;

    @KafkaListener(
            topics = "rota-recebimento-produtos",
            groupId = "consumer-group-v1",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void receiveProducts(String message){
        System.out.println("Received product message: " + message);
    }

}
