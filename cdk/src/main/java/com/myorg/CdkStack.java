package com.myorg;

import software.constructs.Construct;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sns.subscriptions.SqsSubscription;
import software.amazon.awscdk.services.sqs.Queue;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import java.util.Collections;

public class CdkStack extends Stack {
    public CdkStack(final Construct parent, final String id) {
        this(parent, id, null);
    }

    public CdkStack(final Construct parent, final String id, final StackProps props) {
        super(parent, id, props);

        final Queue queue = Queue.Builder.create(this, "CdkQueue")
                .visibilityTimeout(Duration.seconds(300))
                .build();

        final Topic topic = Topic.Builder.create(this, "CdkTopic")
            .displayName("My First Topic Yeah")
            .build();

        topic.addSubscription(new SqsSubscription(queue));
        
        PolicyStatement snsPublishPolicy = PolicyStatement.Builder.create()
        .actions(Collections.singletonList("SNS:Publish"))
        .principals(Collections.singletonList(new ServicePrincipal("s3.amazonaws.com")))
        .resources(Collections.singletonList(topic.getTopicArn()))
        .build();

        topic.addToResourcePolicy(snsPublishPolicy);
    }
}
