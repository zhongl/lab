import org.objectweb.asm.*;
import org.objectweb.asm.commons.AdviceAdapter;
import org.objectweb.asm.commons.Method;

import java.io.PrintStream;
import java.util.Properties;

import static org.objectweb.asm.Opcodes.ASM4;

/**
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
public class InvokeVirtualBySystemProperties {

    public static void main(String[] args) throws Exception {
//        final Advice advice = new Advice();
//        System.getProperties().put("advice", advice);

        ClassWriter cw = new ClassWriter(0);
        ClassReader cr = new ClassReader("X");
        ClassVisitor cv = new ClassVisitor(ASM4, cw) {
            @Override
            public MethodVisitor visitMethod(int i, String s, String s2, String s3, String[] strings) {
                MethodVisitor mv = super.visitMethod(i, s, s2, s3, strings);
                return mv == null ? mv : new AdviceAdapter(ASM4, mv, i, s, s2) {
                    final Type adv = Type.getType(Advice.class);
                    final Type sys = Type.getType(System.class);
                    private int index;

                    @Override
                    public void visitLocalVariable(String name, String desc, String signature, Label start, Label end, int index) {
                        super.visitLocalVariable(name, desc, signature, start, end, index);    //To change body of overridden methods use File | Settings | File Templates.
                    }

                    @Override
                    public void visitCode() {
                        super.visitCode();
                    }

                    @Override
                    protected void onMethodEnter() {
                        index = newLocal(adv);
                        invokeStatic(sys, Method.getMethod("java.util.Properties getProperties()"));
                        push("advice");
                        invokeVirtual(Type.getType(Properties.class), Method.getMethod("Object get(Object)"));
                        storeLocal(index);
                        Label skip1 = new Label();
                        Label skip2 = new Label();

                        loadLocal(index);
                        ifNonNull(skip1);

                        Type print = Type.getType(PrintStream.class);
                        getStatic(sys, "err", print);
                        push("Advice not found");
                        invokeVirtual(print, Method.getMethod("void println(String)"));

                        mark(skip1);

                        loadLocal(index);
                        ifNull(skip2);

                        loadLocal(index);
                        checkCast(adv);
                        push("begin");
                        invokeVirtual(adv, Method.getMethod("void log(String)"));

                        mark(skip2);
                    }


                    @Override
                    protected void onMethodExit(int opcode) {
                        Label skip = newLabel();
                        loadLocal(index);
                        ifNull(skip);

                        loadLocal(index);
                        checkCast(adv);
                        push("end");
                        invokeVirtual(adv, Method.getMethod("void log(String)"));

                        mark(skip);
                    }

                    @Override
                    public void visitMaxs(int maxStack, int maxLocals) {
                        super.visitMaxs(maxStack + 2, maxLocals);    //To change body of overridden methods use File | Settings | File Templates.
                    }
                };
            }
        };
        cr.accept(cv, 0);
        Class<?> aClass = new MyClassLoader().defineClass("X", cw.toByteArray());
        Object o = aClass.newInstance();
        aClass.getMethod("size").invoke(o);
    }

    static class MyClassLoader extends ClassLoader {
        public Class<?> defineClass(String name, byte[] b) {
            return defineClass(name, b, 0, b.length);
        }
    }

    public static class Advice {
        static {
            System.out.println("loaded");
        }

        public void log(String record) {
            System.out.println(record);
        }
    }


}

