import static org.apache.zookeeper.Watcher.Event.EventType.None;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.watch.WatcherMode;

public class EleicaoDeLider {
    private static final String HOST = "localhost";
    private static final String PORTA = "2181";
    private static final int TIMEOUT = 5000;
    private static final String NAMESPACE_ELEICAO = "/eleicao";
    //variável que armazena o nome do ZNodeAtual
    // cada processo que executa uma instância deste programa tem o seu
    private String nomeDoZNodeDesseProcesso;
    private ZooKeeper zooKeeper;
    private static final String ZNODE_TESTE_WATCHER = "/teste_watch";
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
        System.out.println(candidatos);
        //ordena in-place: a coleção é alterada
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

    public void registrarWatcher(){
        TesteWatcher watcher = new TesteWatcher();
        try {
            //Eventos NodeChildrenChanged não serão enviados
            //alterações nos filhos serão representados por NodeCreated, NodeDeleted e NodeDataChanged.
            zooKeeper.addWatch(ZNODE_TESTE_WATCHER, watcher, AddWatchMode.PERSISTENT_RECURSIVE);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

//    public void registrarWatcher() {
//        try {
//            //uma instância da classe TesteWatcher
//            //seu método process será chamado quando um evento acontecer
//            TesteWatcher watcher = new TesteWatcher();
////            zooKeeper.addWatch(ZNODE_TESTE_WATCHER, watcher, AddWatchMode.PERSISTENT_RECURSIVE);
//            Stat stat = zooKeeper.exists(ZNODE_TESTE_WATCHER, watcher);
//            //ZNode existe
//            if (stat != null){
//                //cuidado, se o ZNode não tiver dados o retorno é null
//                //e o construtor da classe String lança uma NPE
//                byte [] bytes = zooKeeper.getData(ZNODE_TESTE_WATCHER, watcher, stat);
//                String dados = bytes != null ? new String(bytes) : "";
//                System.out.println("Dados: " + dados);
//                //se não existirem filhos, o resultado é uma lista vazia
//                List <String> filhos = zooKeeper.getChildren(ZNODE_TESTE_WATCHER, watcher);
//                System.out.println ("Filhos: " + filhos);
//            }
//        }
//        catch (KeeperException | InterruptedException e){
//           e.printStackTrace();
//        }
//    }
    //classe interna
    private class TesteWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            System.out.println(event);
            try{
                switch (event.getType()){
                    case NodeCreated:
                        System.out.println("ZNode criado");
                        break;
                    case NodeDeleted:
                        System.out.println ("ZNode removido");
                        break;
                    case NodeDataChanged:
                        System.out.println ("Dados do ZNode alterados");
                        Stat stat = zooKeeper.exists(ZNODE_TESTE_WATCHER, false);
                        byte [] bytes = zooKeeper.getData(ZNODE_TESTE_WATCHER, false, stat);
                        String dados = bytes != null ? new String (bytes) : "";
                        System.out.println ("Dados: " + dados);
                        break;
                    //Não acontecerá mais
                    case NodeChildrenChanged:
                        System.out.println("Evento envolvendo os filhos");
                        List <String> filhos = zooKeeper.getChildren(ZNODE_TESTE_WATCHER, false);
                        System.out.println(filhos);
                }
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
            //importante!!
            //registrarWatcher();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        EleicaoDeLider eleicaoDeLider = new EleicaoDeLider();
        eleicaoDeLider.conectar();
        eleicaoDeLider.realizarCandidatura();
        eleicaoDeLider.elegerOLider();
        eleicaoDeLider.registrarWatcher();
        eleicaoDeLider.executar();
        eleicaoDeLider.fechar();
    }
}
