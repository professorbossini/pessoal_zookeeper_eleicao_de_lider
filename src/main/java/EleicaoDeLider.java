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
    private static final int TIMEOUT = 15000;
    private static final String NAMESPACE_ELEICAO = "/eleicao";
    //variável que armazena o nome do ZNodeAtual
    // cada processo que executa uma instância deste programa tem o seu
    private String nomeDoZNodeDesseProcesso;
    private MyZooKeeper zooKeeper;
    private static final String ZNODE_TESTE_WATCHER = "/teste_watch";
    public void fechar () throws InterruptedException{
        zooKeeper.close();
    }

    private class MyZooKeeper extends ZooKeeper{

        public MyZooKeeper(String connectString, int sessionTimeout, Watcher watcher) throws IOException {
            super(connectString, sessionTimeout, watcher);
        }

        @Override
        public List<String> getExistWatches() {
            return super.getExistWatches();
        }

        @Override
        protected List<String> getDataWatches() {
            return super.getDataWatches();
        }

        @Override
        protected List<String> getChildWatches() {
            return super.getChildWatches();
        }
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
        zooKeeper = new MyZooKeeper(
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
    String watches = "";
    public void registrarWatcher(){
        try{
            TesteWatcher watcher = new TesteWatcher();
            Stat stat = zooKeeper.exists(ZNODE_TESTE_WATCHER, watcher);
            watches = String.format(
                    "E:%s, D:%s, C:%s\n",
                    zooKeeper.getExistWatches().toString(),
                    zooKeeper.getDataWatches().toString(),
                    zooKeeper.getChildWatches().toString()
            );
            System.out.println("After exists");
            System.out.println(watches);
            if (stat != null){
                byte [] bytes = zooKeeper.getData(ZNODE_TESTE_WATCHER, watcher, stat);
                String dados = bytes != null ? new String(bytes) : "";
                watches = String.format(
                        "E:%s, D:%s, C:%s\n",
                        zooKeeper.getExistWatches().toString(),
                        zooKeeper.getDataWatches().toString(),
                        zooKeeper.getChildWatches().toString()
                );
                System.out.println("After getData");
                System.out.println(watches);
                zooKeeper.getChildren(ZNODE_TESTE_WATCHER, watcher);
                watches = String.format(
                        "E:%s, D:%s, C:%s\n",
                        zooKeeper.getExistWatches().toString(),
                        zooKeeper.getDataWatches().toString(),
                        zooKeeper.getChildWatches().toString()
                );
                System.out.println("After getChildren");
                System.out.println(watches);
            }
        }
        catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    //classe interna
    private class TesteWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            watches = String.format(
                    "E:%s, D:%s, C:%s\n",
                    zooKeeper.getExistWatches().toString(),
                    zooKeeper.getDataWatches().toString(),
                    zooKeeper.getChildWatches().toString()
            );
            System.out.println("Event started");
            System.out.println(watches);
            System.out.println(event);
            switch (event.getType()){
                case NodeCreated:
                    System.out.println("ZNode criado");
                    break;
                case NodeDeleted:
                    System.out.println ("ZNode removido");
                    break;
                case NodeDataChanged:
                    System.out.println ("Dados do ZNode alterados");
                    break;
            }
            watches = String.format(
                    "E:%s, D:%s, C:%s\n",
                    zooKeeper.getExistWatches().toString(),
                    zooKeeper.getDataWatches().toString(),
                    zooKeeper.getChildWatches().toString()
            );
            System.out.println("Event finished");
            System.out.println(watches);
            registrarWatcher();
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
