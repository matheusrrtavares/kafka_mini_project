package producer.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import producer.dto.ProdutoDTO;
import producer.service.ProducerWorkerService;

@RestController
@RequestMapping("/kafka")
@RequiredArgsConstructor
public class ProdutoController {

    private final ProducerWorkerService producerWorker;

    @PostMapping
    public ResponseEntity<String> send(@RequestBody ProdutoDTO dto) {
        producerWorker.producer(dto);
        return ResponseEntity.accepted().body(
                String.format(
                        "Produto '%s' enviado com sucesso pela rota '%s'", dto.content(), dto.route()));
    }

}
