import static org.apache.zookeeper.Watcher.Event.EventType.None;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class EleicaoDeLider {
    private static final String HOST = "localhost";
    private static final String PORTA = "2181";
    private static final int TIMEOUT = 5000;
    private static final String NAMESPACE_ELEICAO = "/eleicao";
    //variável que armazena o nome do ZNodeAtual
    // cada processo que executa uma instância deste programa tem o seu
    private String nomeDoZNodeDesseProcesso;
    private ZooKeeper zooKeeper;

    public void fechar () throws InterruptedException{
        zooKeeper.close();
    }

    public void realizarCandidatura () throws InterruptedException, KeeperException {
        //o prefixo
        String prefixo = String.format("%s/cand_", NAMESPACE_ELEICAO);
        //parâmetros: prefixo, dados a serem armazenados no ZNode, lista de segurança e tipo do ZNode
        String pathInteiro = zooKeeper.create(prefixo, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        //exibe o path inteiro
        System.out.println(pathInteiro);
        //armazena somente o nome do ZNode recém criado
        this.nomeDoZNodeDesseProcesso = pathInteiro.replace(String.format("%s/", NAMESPACE_ELEICAO), "");
    }

    public void elegerOLider () throws InterruptedException, KeeperException {
        //o segundo parâmetro (watch) indica que não queremos registrar um manipulador de eventos
        List <String> candidatos = zooKeeper.getChildren(NAMESPACE_ELEICAO, false);
        //ordena in-place: a coleção é alterada
        Collections.sort(candidatos);
        String oMenor = candidatos.get(0);
        if (oMenor.equals(nomeDoZNodeDesseProcesso))
            System.out.printf("Me chamo %s e sou o líder", nomeDoZNodeDesseProcesso);
        else
            System.out.printf("Me chamo %s e não sou o líder. O líder é o %s.", nomeDoZNodeDesseProcesso, oMenor);
    }

    public void conectar () throws IOException{
        zooKeeper = new ZooKeeper(
                String.format("%s:%s", HOST, PORTA),
                TIMEOUT,
                event -> {
                    if (event.getType() == None){
                        if (event.getState() == Watcher.Event.KeeperState.SyncConnected){
                            System.out.println ("Conectou!!");
                            System.out.printf("Estamos na thread: %s\n", Thread.currentThread().getName());
                        }
                        else if (event.getState() == Watcher.Event.KeeperState.Disconnected){
                            synchronized (zooKeeper){
                                System.out.println ("Desconectou...");
                                System.out.println ("Estamos na thread: " + Thread.currentThread().getName());
                                zooKeeper.notify();
                            }
                        }
                    }
                }
        );
    }
    public void executar() throws  InterruptedException{
        synchronized (zooKeeper){
            zooKeeper.wait();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        EleicaoDeLider eleicaoDeLider = new EleicaoDeLider();
        eleicaoDeLider.conectar();
        eleicaoDeLider.realizarCandidatura();
        eleicaoDeLider.elegerOLider();
        eleicaoDeLider.executar();
        eleicaoDeLider.fechar();
    }
}
