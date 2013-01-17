/*
* JBoss, a division of Red Hat
* Copyright 2012, Red Hat Middleware, LLC, and individual contributors as indicated
* by the @authors tag. See the copyright.txt in the distribution for a
* full listing of individual contributors.
*
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
*
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
*/

package org.gatein.wsrp.handler;

import org.oasis.wsrp.v2.GetMarkup;
import org.oasis.wsrp.v2.GetResource;
import org.oasis.wsrp.v2.HandleEvents;
import org.oasis.wsrp.v2.MarkupResponse;
import org.oasis.wsrp.v2.PerformBlockingInteraction;
import org.oasis.wsrp.v2.ResourceResponse;
import org.oasis.wsrp.v2.RuntimeContext;
import org.oasis.wsrp.v2.SessionContext;
import org.oasis.wsrp.v2.SessionParams;
import org.oasis.wsrp.v2.UpdateResponse;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.ws.LogicalMessage;
import javax.xml.ws.handler.LogicalHandler;
import javax.xml.ws.handler.LogicalMessageContext;
import javax.xml.ws.handler.MessageContext;

/** @author <a href="mailto:chris.laprun@jboss.com">Chris Laprun</a> */
public class SessionIdLoggingHandler implements LogicalHandler<LogicalMessageContext>
{
   @Override
   public boolean handleMessage(LogicalMessageContext context)
   {
      try
      {
         // outbound message means response on the producer
         if (Boolean.TRUE.equals(context.get(MessageContext.MESSAGE_OUTBOUND_PROPERTY)))
         {
            LogicalMessage msg = context.getMessage();
            JAXBContext jaxbContext = JAXBContext.newInstance(MarkupResponse.class, ResourceResponse.class, UpdateResponse.class);
            return handleResponse(msg.getPayload(jaxbContext));
         }
         else
         {
            LogicalMessage msg = context.getMessage();
            JAXBContext jaxbContext = JAXBContext.newInstance(GetMarkup.class, GetResource.class, HandleEvents.class, PerformBlockingInteraction.class);
            return handleRequest(msg.getPayload(jaxbContext));
         }
      }
      catch (JAXBException e)
      {
         throw new RuntimeException(e);
      }
   }

   private boolean handleResponse(Object context)
   {
      final SessionContext sessionContext = getSessionContext(context);
      if(sessionContext != null)
      {
         final String sessionID = sessionContext.getSessionID();
         System.out.println("Producer set sessionID in " + context.getClass().getSimpleName() + " = " + sessionID);
      }

      return true;
   }

   private SessionContext getSessionContext(Object context)
   {
      if (context instanceof MarkupResponse)
      {
         return ((MarkupResponse)context).getSessionContext();
      }
      else if (context instanceof ResourceResponse)
      {
         return ((ResourceResponse)context).getSessionContext();
      }
      else if (context instanceof UpdateResponse)
      {
         return ((UpdateResponse)context).getSessionContext();
      }
      else
      {
         return null;
      }
   }

   private boolean handleRequest(Object context)
   {
      SessionParams sessionParams = getSessionParams(context);
      if(sessionParams != null)
      {
         final String sessionID = sessionParams.getSessionID();
         System.out.println("Consumer is sending sessionID in " + context.getClass().getSimpleName() + " = " + sessionID);
      }
      return true;
   }

   private SessionParams getSessionParams(Object context)
   {
      RuntimeContext runtimeContext = null;
      if(context instanceof GetMarkup)
      {
         runtimeContext = ((GetMarkup)context).getRuntimeContext();
      }
      else if (context instanceof GetResource)
      {
         runtimeContext = ((GetResource)context).getRuntimeContext();
      }
      else if (context instanceof HandleEvents)
      {
         runtimeContext = ((HandleEvents)context).getRuntimeContext();
      }
      else if (context instanceof PerformBlockingInteraction)
      {
         runtimeContext = ((PerformBlockingInteraction) context).getRuntimeContext();
      }

      if(runtimeContext != null)
      {
         return runtimeContext.getSessionParams();
      }
      else
      {
         return null;
      }
   }

   @Override
   public boolean handleFault(LogicalMessageContext context)
   {
      return true;
   }

   @Override
   public void close(MessageContext context)
   {
      // nothing
   }
}
